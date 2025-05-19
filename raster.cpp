/**
 * @file rasterSafety.cpp
 * @author Refactored from original by kongxinglong
 * @brief 光栅安全处理逻辑源文件 - 使用状态机重构
 */

// 包含必要的头文件
#include "rasterSafety.h"
#include "../nrcAPI.h" // 假设提供 NRC_* 函数
#include <spdlog/spdlog.h>
#include <spdlog/sinks/stdout_color_sinks.h>
#include <spdlog/sinks/rotating_file_sink.h>
#include <nlohmann/json.hpp> // 文件配置使用 nlohmann/json，外部API使用 JsonCpp
#include <iostream>
#include <mutex>
#include <atomic>
#include <thread>
#include <fstream>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>
#include <errno.h>
#include <cstring>
#include <signal.h>
#include <map>          // 包含 map 用于 robot_states
#include <string>       // 用于 std::string 和 std::to_string
#include <sstream>      // 备用，某些复杂拼接可能用得上
#include <chrono>       // 用于 std::chrono::milliseconds

// 文件配置使用 nlohmann/json
using json = nlohmann::json;

// 内部全局变量
namespace {
    // 配置目录和文件名
    const std::string CONFIG_DIR = "raster_config";
    const std::string CONFIG_FILE_NAME = "raster_safety_config.json"; // 假设机器人对使用一个配置文件

    // 状态机状态
    enum SystemState {
        SYSTEM_STATE_NORMAL,  // 无安全触发激活，机器人可运行
        SYSTEM_STATE_LIMITED  // 安全触发激活，机器人已暂停
    };

    // 系统状态变量 (原子变量，用于在主逻辑锁之外安全读取)
    std::atomic<SystemState> current_system_state{SYSTEM_STATE_NORMAL};

    // 机器人状态 - 受控机器人各自的信息
    struct RobotState {
        int current_run_status;      // 0:停止 1:暂停 2:运行 (来自 NRC 的快照)
        std::string last_job_name;   // 由此安全模块暂停的作业名
        bool message_sent_limited;   // 在 LIMITED 状态周期内，发送暂停消息的标志
        bool message_sent_recovered; // 在 LIMITED 恢复到 NORMAL 状态周期内，发送恢复消息的标志

        RobotState() : current_run_status(0), message_sent_limited(false), message_sent_recovered(false) {}
    };
    // map 机器人ID (1, 2) 到其状态. 受 io_mutex 保护.
    std::map<int, RobotState> robot_states;

    // IO 配置 - 按 IO 号索引 (0-2048)
    // 未配置条目使用结构体的默认构造函数 (-1 io_index, is_configured=false)
    std::vector<IOConfig> io_list_indexed(2049);

    // 存储配置文件或API传入的限速值 (实际动作总是暂停/恢复)
    int configured_limited_speed = 30;

    // 线程管理
    // 互斥锁，用于访问 io_list_indexed, configured_limited_speed, robot_states, current_system_state (更新时)
    std::mutex io_mutex;
    std::thread* monitor_thread = nullptr;     // IO 监测线程指针
    std::atomic<bool> thread_running{true};    // 线程运行控制标志

    // 日志记录
    std::shared_ptr<spdlog::logger> file_logger;
    const size_t LOG_FILE_SIZE = 1024 * 1024 * 20;  // 20MB
    const size_t LOG_FILES_COUNT = 3;               // 保留 3 个文件

    // 由此实例处理的机器人ID. 根据上下文假定是 1 和 2.
    const std::vector<int> handled_robot_ids = {1, 2};

    // 暂停/恢复操作后等待确认状态的时间 (毫秒)
    const int STATE_CONFIRM_WAIT_MS = 200;

} // 匿名命名空间结束

// --- 内部函数前向声明 ---
// 放在这里，确保在使用它们的地方之前已经被声明

static bool read_io(int index);
static bool createDirectory(const std::string& path);
static bool fileExists(const std::string& path);
static bool setFilePermissions(const std::string& path);
static bool save_to_file();
static bool load_from_file();
static void io_monitor_thread();
static void pause_robots();
static void resume_robots();

// 声明服务停止函数
void stopRasterSafetyService();
// 声明信号处理函数 (现在放在使用它的函数之前)
static void handle_shutdown_signal(int signal);

// Helper to get/initialize robot state. Access to map needs mutex.
static RobotState& getRobotState(int robot_id) {
    // 调用者需确保已持有 io_mutex，如果需要修改 map 或并发访问.
    // 如果在锁定区域外调用 (例如初始化期间)，并且只有一个线程调用，则是安全的.
    // 在监测线程和控制函数中，在锁内调用.
    if (robot_states.find(robot_id) == robot_states.end()) {
        robot_states[robot_id] = RobotState();
        // 初始化时获取当前状态和作业名 (如果运行/暂停) - NRC 调用可能需要锁，取决于其线程安全性
        // 假设 NRC 调用在 io_mutex 持有期间是安全的.
        robot_states[robot_id].current_run_status = NRC_Rbt_GetProgramRunStatus(robot_id);
        // NRC_GetCurrentOpenJob 可能在无作业打开/运行/暂停时失败.
        std::string initial_job_name;
        int get_job_ret = NRC_GetCurrentOpenJob(robot_id, initial_job_name);
        if(get_job_ret == 0 && (robot_states[robot_id].current_run_status == 1 || robot_states[robot_id].current_run_status == 2)) {
             // If already running or paused AND we successfully got a job name, maybe store it?
             // However, we only need the job name if *we* paused it. Storing it here might be misleading.
             // Let's stick to only storing job name when *we* pause.
        }

        if(file_logger) SPDLOG_INFO("已初始化机器人 {} 的状态结构体.", robot_id);
    }
    return robot_states.at(robot_id); // 使用 at() 以在查找/插入后更安全地访问
}

// 辅助函数: 读取布尔量 IO 状态. 假设 NRC_ReadTcpBoolVar 读取操作是线程安全的.
static bool read_io(int index) {
    if (index < 0 || index > 2048) {
        if(file_logger) SPDLOG_WARN("尝试读取无效 IO 索引: {}", index);
        return false; // 无效索引
    }
    // 假设 NRC_ReadTcpBoolVar 内部处理无效索引或返回默认值如 false
    return NRC_ReadTcpBoolVar(index);
}

// 动作: 暂停机器人. 假定调用者已持有 io_mutex.
static void pause_robots() {
    SPDLOG_INFO("[动作] 因安全触发启动机器人暂停操作. 系统状态: 安全受限.");

    for (int id : handled_robot_ids) {
        auto& state = getRobotState(id); // 通过辅助函数访问状态 (调用者持有 mutex)

        // 执行动作前刷新状态
        state.current_run_status = NRC_Rbt_GetProgramRunStatus(id);

        if (state.current_run_status == 2) { // 只在运行时暂停
            // 在暂停 *之前* 获取当前作业名，以便知道恢复哪个作业
            std::string current_job;
            int get_job_ret = NRC_GetCurrentOpenJob(id, current_job);
            state.last_job_name = current_job; // 无论 NRC_GetCurrentOpenJob 是否成功，都存储作业名 (可能为空)
            if(get_job_ret != 0) {
                 if(file_logger) SPDLOG_WARN("获取机械臂 {} 作业名失败. NRC_GetCurrentOpenJob 返回 {}", id, get_job_ret);
            } else {
                 if(file_logger) SPDLOG_INFO("机械臂 {} 当前作业名为: {}", id, state.last_job_name);
            }

            // 调用暂停接口 (不依赖其返回值判断成功)
            int ret_pause_call = NRC_Rbt_PauseRunJobfile(id);
            if(file_logger) SPDLOG_INFO("调用 NRC_Rbt_PauseRunJobfile({}) 返回: {}", id, ret_pause_call);

            // 短暂等待，让状态更新
            std::this_thread::sleep_for(std::chrono::milliseconds(STATE_CONFIRM_WAIT_MS));

            // 再次查询状态，确认是否进入暂停状态
            int new_status = NRC_Rbt_GetProgramRunStatus(id);
             if(file_logger) SPDLOG_INFO("等待 {}ms 后，机械臂 {} 新状态为: {}", STATE_CONFIRM_WAIT_MS, id, new_status);

            if (new_status == 1) { // 暂停成功 (达到了暂停状态)
                if (!state.message_sent_limited) {
                    std::string msg = "安全触发，机械臂" + std::to_string(id) + "因安全IO动作被暂停";
                    NRC_TriggerErrorReport(1, msg); // 安全触发的报警级别 1
                    if(file_logger) SPDLOG_INFO(msg);
                    state.message_sent_limited = true;
                    state.message_sent_recovered = false; // 重置恢复标志
                } else {
                    if(file_logger) SPDLOG_DEBUG("机械臂 {} 在当前安全受限阶段已发送过暂停消息.", id);
                }
            } else { // 暂停失败 (未能达到暂停状态)
                 std::string msg = "安全触发，尝试暂停机械臂" + std::to_string(id) + "失败！未能达到暂停状态。暂停前状态:" + std::to_string(state.current_run_status) + ", 调用返回:" + std::to_string(ret_pause_call) + ", 暂停后状态:" + std::to_string(new_status);
                 // 无论 message_sent_limited 标志如何，都会发送此错误报告，因为这是动作失败
                 NRC_TriggerErrorReport(3, msg); // 失败的更高级别
                 if(file_logger) SPDLOG_ERROR(msg);
                 // 如果暂停失败，清除记录的 job name，避免下次尝试恢复一个未能被我们成功暂停的作业
                 state.last_job_name.clear();
            }
        } else { // 机器人已停止 (0) 或暂停 (1)
             if (!state.message_sent_limited) {
                std::string msg_status = (state.current_run_status == 1) ? "暂停" : "停止";
                std::string msg = "安全触发，机械臂" + std::to_string(id) + "已处于" + msg_status + "状态，无需暂停";
                NRC_TriggerErrorReport(0, msg); // 信息级别通知
                if(file_logger) SPDLOG_INFO(msg);
                state.message_sent_limited = true;
                state.message_sent_recovered = false; // 重置恢复标志
             } else {
                 if(file_logger) SPDLOG_DEBUG("机械臂 {} 已停止/暂停，并在当前安全受限阶段发送过暂停消息.", id);
             }
             // 如果未运行，清空作业名，因为它不是我们暂停的
             state.last_job_name.clear();
        }
    }
    // 调用者释放 mutex
}

// 动作: 恢复机器人. 假定调用者已持有 io_mutex.
static void resume_robots() {
    SPDLOG_INFO("[动作] 安全触发解除后启动机器人恢复操作. 系统状态: 正常.");

    for (int id : handled_robot_ids) {
        auto& state = getRobotState(id); // 通过辅助函数访问状态 (调用者持有 mutex)

        // 执行动作前刷新状态
        state.current_run_status = NRC_Rbt_GetProgramRunStatus(id);

        if (state.current_run_status == 1) { // 只在暂停时尝试恢复
            if (!state.last_job_name.empty()) {
                // 尝试恢复我们记录时暂停的作业
                if(file_logger) SPDLOG_INFO("尝试恢复机械臂 {} 作业: {}", id, state.last_job_name);

                // 调用恢复接口 (不依赖其返回值判断成功)
                int ret_resume_call = NRC_StartRunJobfile(state.last_job_name.c_str()); // NRC_StartRunJobfile 接受 const char*
                if(file_logger) SPDLOG_INFO("调用 NRC_StartRunJobfile({}) 返回: {}", state.last_job_name, ret_resume_call);


                // 短暂等待，让状态更新
                std::this_thread::sleep_for(std::chrono::milliseconds(STATE_CONFIRM_WAIT_MS));

                // 再次查询状态，确认是否进入运行状态
                int new_status = NRC_Rbt_GetProgramRunStatus(id);
                 if(file_logger) SPDLOG_INFO("等待 {}ms 后，机械臂 {} 新状态为: {}", STATE_CONFIRM_WAIT_MS, id, new_status);


                 if (new_status == 2) { // 恢复成功 (达到了运行状态)
                    if (!state.message_sent_recovered) {
                        std::string msg = "安全触发解除，机械臂" + std::to_string(id) + "作业已恢复";
                        NRC_TriggerErrorReport(0, msg); // 恢复的信息级别
                        if(file_logger) SPDLOG_INFO(msg);
                        state.message_sent_recovered = true;
                        state.message_sent_limited = false; // 重置暂停标志
                    } else {
                         if(file_logger) SPDLOG_DEBUG("机械臂 {} 在当前正常阶段已发送过恢复消息.", id);
                    }
                    state.last_job_name.clear(); // 尝试成功恢复后清除作业名
                 } else {
                    // 恢复失败 (未能达到运行状态)
                    std::string msg = "安全触发解除，尝试恢复机械臂" + std::to_string(id) + "作业失败！未能达到运行状态。暂停前状态:" + std::to_string(state.current_run_status) + ", 调用返回:" + std::to_string(ret_resume_call) + ", 恢复后状态:" + std::to_string(new_status);
                    // 无论 message_sent_recovered 标志如何，都会发送此错误报告，因为这是动作失败
                    NRC_TriggerErrorReport(3, msg); // 失败的更高级别
                    if(file_logger) SPDLOG_ERROR(msg);
                    // 如果恢复失败，状态仍为暂停 (状态 1)，作业名不清除，以便下次可能再次尝试恢复.
                 }
            } else {
                // 暂停，但没有我们记录的作业名. 不是我们暂停的.
                if (!state.message_sent_recovered) {
                     std::string msg = "安全触发解除，机械臂" + std::to_string(id) + "处于暂停状态但无记录的作业，需手动恢复";
                     NRC_TriggerErrorReport(0, msg); // 信息级别通知
                     if(file_logger) SPDLOG_WARN(msg);
                     state.message_sent_recovered = true; // 防止重复消息
                     state.message_sent_limited = false;
                } else {
                    if(file_logger) SPDLOG_DEBUG("机械臂 {} 无记录作业而暂停，并在当前正常阶段发送过恢复消息.", id);
                }
            }
        } else { // 机器人已停止 (0) 或运行 (2)
             if (!state.message_sent_recovered) {
                 std::string msg_status = (state.current_run_status == 2) ? "运行" : "停止";
                 std::string msg = "安全触发解除，机械臂" + std::to_string(id) + "已处于" + msg_status + "状态，无需恢复";
                 NRC_TriggerErrorReport(0, msg); // 信息级别通知
                 if(file_logger) SPDLOG_INFO(msg);
                 state.message_sent_recovered = true;
                 state.message_sent_limited = false;
             } else {
                 if(file_logger) SPDLOG_DEBUG("机械臂 {} 已停止/运行，并在当前正常阶段发送过恢复消息.", id);
             }
             // 如果机器人通过其他方式恢复/停止，则清除作业名
             state.last_job_name.clear();
        }
    }
    // 调用者释放 mutex
}


static bool createDirectory(const std::string& path) {
    struct stat st;
    if (stat(path.c_str(), &st) == -1) {
        if (mkdir(path.c_str(), 0755) == -1) {
            if (errno != EEXIST) {
                std::cerr << "[光栅安全控制] 创建目录失败: " + std::string(strerror(errno)) << std::endl;
                if(file_logger) SPDLOG_ERROR("创建目录失败: {}, 错误: {}", path, strerror(errno));
                return false;
            }
        } else {
            if(file_logger) SPDLOG_INFO("目录已创建: {}", path);
        }
    } else {
         if(file_logger) SPDLOG_INFO("目录已存在: {}", path);
    }
    return true;
}

static bool fileExists(const std::string& path) {
    struct stat buffer;
    return (stat(path.c_str(), &buffer) == 0);
}

static bool setFilePermissions(const std::string& path) {
    if (chmod(path.c_str(), S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH) == -1) { // rw-r--r--
        std::cerr << "[光栅安全控制] 设置文件权限失败: " + std::string(strerror(errno)) << std::endl;
        if(file_logger) SPDLOG_ERROR("设置文件权限失败: {}, 错误: {}", path, strerror(errno));
        return false;
    }
    if(file_logger) SPDLOG_INFO("文件权限设置成功: {}", path);
    return true;
}

// 保存当前配置 (io_list_indexed 和 configured_limited_speed) 到文件
// 假定调用者已持有 io_mutex.
static bool save_to_file() {
    try {
        if (!createDirectory(CONFIG_DIR)) {
            return false; // createDirectory 已记录错误
        }

        std::string filename = CONFIG_DIR + "/" + CONFIG_FILE_NAME;
        if(file_logger) SPDLOG_INFO("保存配置到文件: {}", filename);

        json j;
        j["last_update"] = std::time(nullptr);
        j["io_config"] = json::array();

        // 遍历索引列表查找已配置的 IO
        for (int i = 0; i < io_list_indexed.size(); ++i) {
            const auto& cfg = io_list_indexed[i];
            if (cfg.is_configured) {
                 json io_item;
                 io_item["io_index"] = cfg.io_index;
                 io_item["reset_io_index"] = cfg.reset_io_index;
                 io_item["trigger_value"] = cfg.trigger_value; // 保存存储的 int 值 (0 或 1)
                 io_item["description"] = cfg.description;
                 j["io_config"].push_back(io_item);
            }
        }

        j["limited_speed"] = configured_limited_speed; // 保存配置的值

        std::ofstream file(filename.c_str());
        if (!file) {
            std::cerr << "[光栅安全控制] 无法打开配置文件进行写入: " + filename << std::endl;
            if(file_logger) SPDLOG_ERROR("无法打开配置文件进行写入: {}", filename);
            return false;
        }

        file << j.dump(4); // 漂亮打印，缩进 4 个空格
        file.close();

        if (!setFilePermissions(filename)) {
            // setFilePermissions 已记录错误
        }

        return true;
    } catch (const std::exception& e) {
        std::cerr << "[光栅安全控制] 保存配置文件失败: " + std::string(e.what()) << std::endl;
        if(file_logger) SPDLOG_ERROR("保存配置文件失败: {}", e.what());
        return false;
    }
}

// 从文件加载配置到 io_list_indexed 和 configured_limited_speed
// 假定调用者已持有 io_mutex.
static bool load_from_file() {
    std::string filename = CONFIG_DIR + "/" + CONFIG_FILE_NAME;

    if (!fileExists(filename)) {
        // 如果目录不存在则创建
        if (!createDirectory(CONFIG_DIR)) {
            // createDirectory 已记录错误
             if(file_logger) SPDLOG_ERROR("加载配置失败: 创建目录失败.");
            return false; // 没有目录无法继续
        }

        // 如果找不到配置文件，则创建一个默认文件
        if(file_logger) SPDLOG_INFO("找不到配置文件，创建默认文件: {}", filename);
        json j;
        j["last_update"] = std::time(nullptr);
        j["io_config"] = json::array(); // 空配置
        j["limited_speed"] = configured_limited_speed; // 默认速度 30

        std::ofstream file(filename.c_str());
        if (!file) {
            std::cerr << "[光栅安全控制] 无法创建新配置文件: " + filename << std::endl;
            if(file_logger) SPDLOG_ERROR("无法创建新配置文件: {}", filename);
            return false;
        }
        file << j.dump(4);
        file.close();

        if (!setFilePermissions(filename)) {
            // setFilePermissions 已记录错误
        }
        // 默认配置现在隐式加载 (io_list_indexed 已默认初始化)
        if(file_logger) SPDLOG_INFO("默认配置文件已创建，默认设置已生效.");
        return true;
    }

    std::ifstream file(filename.c_str());
    if (!file) {
        std::cerr << "[光栅安全控制] 无法打开配置文件: " + filename << std::endl;
        if(file_logger) SPDLOG_ERROR("无法打开配置文件: {}", filename);
        return false;
    }

    json j;
    bool json_load_ok = true;
    try {
        file >> j;
    } catch(const std::exception& e) {
        std::cerr << "[光栅安全控制] 配置文件解析失败: " + std::string(e.what()) << std::endl;
        if(file_logger) SPDLOG_ERROR("配置文件解析失败: {}", e.what());
        file.close();
        return false;
    }
    file.close();

    if(file_logger) SPDLOG_INFO("成功读取配置文件: {}", filename);

    // 将 io_list_indexed 重置为默认状态 (所有未配置)
    io_list_indexed.assign(2049, IOConfig()); // 使用默认构造函数: io_index=-1, is_configured=false

    if (j.contains("io_config") && j["io_config"].is_array()) {
        const auto& io_config = j["io_config"];
        for (const auto& item : io_config) {
            if (item.is_object() && item.contains("io_index") && item["io_index"].is_number_integer()) {
                int io_index = item["io_index"].get<int>();

                if (io_index >= 0 && io_index < io_list_indexed.size()) {
                    // 使用加载的数据创建 IOConfig 对象
                    IOConfig cfg;
                    cfg.io_index = io_index;
                    cfg.reset_io_index = item.value("reset_io_index", 0);
                    cfg.trigger_value = item.value("trigger_value", 1); // 默认触发高电平
                    cfg.description = item.value("description", "");
                    cfg.already_triggered = false; // 加载时总是未触发
                    cfg.trigger_time = 0;
                    cfg.is_configured = true; // 标记为已配置

                    // 验证 reset_io_index 范围
                    if (cfg.reset_io_index < 0 || cfg.reset_io_index > 2048) {
                         if(file_logger) SPDLOG_WARN("配置文件中IO {} 的复位IO索引 {} 无效，将复位IO索引设为 0.", cfg.io_index, cfg.reset_io_index);
                         cfg.reset_io_index = 0; // 修正无效的复位索引
                    }

                    io_list_indexed[io_index] = cfg; // 存储到索引列表
                    if(file_logger) {
                         std::string debug_msg = "加载 IO 配置: 索引" + std::to_string(cfg.io_index) +
                                                 ", 复位=" + std::to_string(cfg.reset_io_index) +
                                                 ", 触发值=" + std::to_string(cfg.trigger_value) +
                                                 ", 描述='" + cfg.description + "'";
                         SPDLOG_DEBUG(debug_msg);
                    }
                } else {
                    if(file_logger) SPDLOG_WARN("配置文件中无效的 IO 索引 {}，跳过此条目.", io_index);
                }
            } else {
                 if(file_logger) SPDLOG_WARN("io_config 数组中无效的条目: 不是对象或缺少/无效 'io_index'");
            }
        }
    } else {
         if(file_logger) SPDLOG_WARN("配置文件不包含有效的 'io_config' 数组.");
    }

    // 加载 configured_limited_speed
    configured_limited_speed = j.value("limited_speed", configured_limited_speed);
    // 注意: 从配置中加载的 'limited_speed' 值被存储，但实际动作总是暂停 (速度 0)
    if (configured_limited_speed < 0 || configured_limited_speed > 100) {
         if(file_logger) SPDLOG_WARN("从文件加载的 configured_limited_speed {} 无效，使用默认值 {}.", configured_limited_speed, 30);
         configured_limited_speed = 30; // 如果加载的值无效，重置为有效的默认值
    }


    if(file_logger) SPDLOG_INFO("成功从文件加载配置. 配置的限速: {}%", configured_limited_speed);
    return true;
}

// 主状态机循环
static void io_monitor_thread() {
    std::cout << "[光栅安全控制] IO监测线程启动!" << std::endl;
    if(file_logger) SPDLOG_INFO("[光栅安全控制] IO监测线程启动!");

    // 如果机器人状态在服务启动时未初始化 (与服务启动中的逻辑重复，但更安全)
    {
        std::lock_guard<std::mutex> lock(io_mutex);
        for(int id : handled_robot_ids) {
            getRobotState(id); // 确保 map 条目存在
        }
    }


    while (thread_running) {
        SystemState required_state = SYSTEM_STATE_NORMAL;
        bool any_io_currently_meets_trigger = false; // 检查当前物理状态
        bool any_io_has_already_triggered_flag = false; // 检查内部状态标志

        { // --- 状态机逻辑的锁范围 ---
            std::lock_guard<std::mutex> lock(io_mutex); // 保护 IO 配置、状态和系统状态 (更新时)

            // 步骤 1: 评估物理 IO 状态并更新内部 `already_triggered` 标志
            for (int i = 0; i < io_list_indexed.size(); ++i) {
                auto& io = io_list_indexed[i]; // 获取引用以便修改

                if (!io.is_configured) continue; // 跳过未配置的条目

                bool current_io_value = read_io(io.io_index); // 假定 read_io 在此足够线程安全

                // 如果此 IO 的触发条件当前满足
                if (current_io_value == (io.trigger_value == 1)) { // 布尔值与 trigger_value (0 或 1) 比较
                    any_io_currently_meets_trigger = true;
                    // 如果这对特定 IO 是一个 *新的* 触发事件
                    if (!io.already_triggered) {
                        io.already_triggered = true; // 设置标志
                        io.trigger_time = std::time(nullptr);
                        if(file_logger) {
                             std::string log_msg = "安全 IO 已触发: 索引 " + std::to_string(io.io_index) +
                                                   " (描述: " + io.description + "), 配置触发值是 " + std::to_string(io.trigger_value) +
                                                   ", 当前值是 " + (current_io_value ? "1" : "0") + ".";
                             SPDLOG_WARN(log_msg);
                        }
                        // 特定 IO 触发的错误报告可以在此处发送，或者等待系统状态转换
                        // 这里记录特定 IO 触发，通用系统状态转换报告稍后发送.
                    }
                } else {
                    // 此特定 IO 的触发条件不再满足.
                    // 如果此 IO 先前已触发 (标志已设置)，检查复位条件.
                    if (io.already_triggered) {
                        bool meets_reset_condition = false;
                        if (io.reset_io_index > 0) {
                            // 已配置复位 IO，检查其状态
                            bool reset_io_value = read_io(io.reset_io_index); // 假定 read_io 安全
                            meets_reset_condition = reset_io_value == true; // 假定复位 IO 触发高电平 (True)
                             if(file_logger) {
                                 std::string debug_msg = "检查已触发 IO " + std::to_string(io.io_index) + " 的复位 IO " + std::to_string(io.reset_io_index) +
                                                         ". 复位 IO 值: " + (reset_io_value ? "1" : "0") + ". 满足复位条件: " + (meets_reset_condition ? "true" : "false");
                                 SPDLOG_DEBUG(debug_msg);
                             }
                        } else {
                            // 没有专用复位 IO，只需清除触发条件就足以允许复位
                            meets_reset_condition = true;
                             if(file_logger) {
                                 std::string debug_msg = "已触发 IO " + std::to_string(io.io_index) + " 没有专用复位 IO. 满足复位条件: " + (meets_reset_condition ? "true" : "false");
                                 SPDLOG_DEBUG(debug_msg);
                             }
                        }

                        if (meets_reset_condition) {
                            io.already_triggered = false; // 清除标志
                            io.trigger_time = 0; // 重置触发时间
                            if(file_logger) {
                                std::string log_msg = "安全 IO 已复位: 索引 " + std::to_string(io.io_index) +
                                                      " (描述: " + io.description + "). 复位条件满足 (复位 IO: " + std::to_string(io.reset_io_index) + ").";
                                SPDLOG_INFO(log_msg);
                            }
                            // 系统恢复在 *所有* 标志清除时发生
                        }
                        // 如果触发条件为 false 但复位条件也为 false，
                        // already_triggered 标志保持为 true，等待复位条件.
                    }
                }

                // 评估触发和复位条件后，检查标志是否已设置
                if (io.already_triggered) {
                     any_io_has_already_triggered_flag = true;
                }
            }

            // 步骤 2: 根据内部 `already_triggered` 标志确定所需的系统状态
            // 如果 *任何一个* 已配置的 IO 的 `already_triggered` 标志已设置，则系统应为 LIMITED.
            required_state = any_io_has_already_triggered_flag ? SYSTEM_STATE_LIMITED : SYSTEM_STATE_NORMAL;

            // 步骤 3: 如果需要，执行状态转换动作
            SystemState previous_state = current_system_state.load(std::memory_order_acquire); // 原子读取
            if (previous_state != required_state) {
                if(file_logger) {
                    std::string log_msg = "检测到系统状态变化: ";
                    log_msg += (previous_state == SYSTEM_STATE_NORMAL ? "正常" : "安全受限");
                    log_msg += " -> ";
                    log_msg += (required_state == SYSTEM_STATE_NORMAL ? "正常" : "安全受限");
                    SPDLOG_INFO(log_msg);
                }

                current_system_state.store(required_state, std::memory_order_release); // 原子更新

                if (required_state == SYSTEM_STATE_LIMITED) {
                    // 转换为 LIMITED
                    pause_robots(); // 动作: 暂停机器人 (在锁定区域内)
                } else { // 转换为 NORMAL (required_state 是 NORMAL)
                    // 注意: 恢复只有在所有 `already_triggered` 标志都为 false 时才会发生，
                    // 这也是 required_state 变为 NORMAL 的原因.
                    resume_robots(); // 动作: 恢复机器人 (在锁定区域内)
                }
            } else {
                 // 如果需要调试，可以周期性记录当前状态，但要避免刷屏
                 // if(file_logger) SPDLOG_DEBUG("系统状态保持: {}", current_system_state.load() == SYSTEM_STATE_NORMAL ? "正常" : "安全受限");
            }

            // 步骤 4: 周期性更新 map 中的机器人状态 (可选，也可在动作中按需进行)
            // 周期性更新确保 robot_states.current_run_status 相对新鲜.
             for(int id : handled_robot_ids) {
                 getRobotState(id).current_run_status = NRC_Rbt_GetProgramRunStatus(id);
             }

        } // --- 锁范围结束 ---

        std::this_thread::sleep_for(std::chrono::milliseconds(50)); // 监测频率
    }

    std::cout << "[光栅安全控制] IO监测线程退出!" << std::endl;
    if(file_logger) SPDLOG_INFO("[光栅安全控制] IO监测线程退出!");
}

// --- 对外接口实现 ---

bool updateIOConfig(const std::vector<IOConfig>& config, int limited_speed) {
    std::lock_guard<std::mutex> lock(io_mutex); // 保护 io_list_indexed 和 configured_limited_speed
    try {
        if (limited_speed < 0 || limited_speed > 100) {
            if(file_logger) SPDLOG_WARN("更新时提供的限速值无效: {}", limited_speed);
            return false;
        }

        configured_limited_speed = limited_speed; // 存储配置的值

        // 清除索引列表中的现有配置
        io_list_indexed.assign(2049, IOConfig()); // 重置所有为未配置 (-1 io_index, is_configured=false)
        if(file_logger) SPDLOG_INFO("已清除内存中的现有 IO 配置.");


        // 应用输入向量中的新配置
        for (const auto& cfg_in : config) {
            // 检查 IO 索引有效性
            if (cfg_in.io_index >= 0 && cfg_in.io_index <= 2048) {
                // 确保复位 IO 索引也有效或为 0
                if (cfg_in.reset_io_index >= 0 && cfg_in.reset_io_index <= 2048) {
                     // 根据输入创建新的 IOConfig 对象，但状态重置
                     IOConfig cfg_new;
                     cfg_new.io_index = cfg_in.io_index;
                     cfg_new.reset_io_index = cfg_in.reset_io_index;
                     cfg_new.trigger_value = cfg_in.trigger_value; // 存储 0 或 1
                     cfg_new.description = cfg_in.description;
                     cfg_new.is_configured = true; // 标记为已配置
                     cfg_new.already_triggered = false; // 更新时总是重置
                     cfg_new.trigger_time = 0;

                     io_list_indexed[cfg_new.io_index] = cfg_new; // 存储到索引列表
                     if(file_logger) {
                         std::string debug_msg = "已应用 IO " + std::to_string(cfg_new.io_index) + " 的新配置: 复位=" + std::to_string(cfg_new.reset_io_index) +
                                                 ", 触发值=" + std::to_string(cfg_new.trigger_value) + ", 描述='" + cfg_new.description + "'";
                         SPDLOG_DEBUG(debug_msg);
                     }


                } else {
                    if(file_logger) SPDLOG_WARN("更新配置中 IO {} 的复位 IO 索引 {} 无效，跳过此配置条目.", cfg_in.io_index, cfg_in.reset_io_index);
                }
            } else {
                 if(file_logger) SPDLOG_WARN("更新配置向量中无效的 IO 索引 {}，跳过条目.", cfg_in.io_index);
            }
        }

        // 保存到文件
        bool saved = save_to_file(); // save_to_file 内部获取 mutex，此处没问题

        // 监测线程将在下一循环检测到所有 already_triggered 标志已清除，
        // 并在需要时 (即如果当前系统状态为 LIMITED 且物理 IO 安全) 触发恢复.

        if (saved) {
            if(file_logger) {
                std::string log_msg = "IO 配置已成功更新到内存和文件. 配置的限速: " + std::to_string(limited_speed) + "%";
                SPDLOG_INFO(log_msg);
            }
        } else {
             if(file_logger) {
                 std::string error_msg = "IO 配置已更新到内存，但保存到文件失败: " + CONFIG_DIR + "/" + CONFIG_FILE_NAME + ". 配置在内存中已激活.";
                 SPDLOG_ERROR(error_msg);
             }
        }
        return saved; // 返回保存状态
    } catch (const std::exception& e) {
        std::cerr << "[光栅安全控制] 更新 IO 配置出错: " + std::string(e.what()) << std::endl;
        if(file_logger) SPDLOG_ERROR("更新 IO 配置出错: {}", e.what());
        return false;
    }
    // 返回时释放 Mutex
}

// 对外函数: 清除内部触发标志并尝试恢复机器人运行
bool resetSpeed() {
    std::lock_guard<std::mutex> lock(io_mutex); // 保护 io_list_indexed 和状态

    SPDLOG_INFO("[复位] 收到外部 resetSpeed 命令.");

    bool was_limited = (current_system_state.load(std::memory_order_acquire) == SYSTEM_STATE_LIMITED);
    bool trigger_flags_cleared = false;

    // 步骤 1: 清除所有已配置 IO 的内部 `already_triggered` 标志.
    // 这允许状态机评估 *当前* 物理状态.
    for (auto& io : io_list_indexed) {
        if (io.is_configured && io.already_triggered) {
            io.already_triggered = false;
            io.trigger_time = 0; // 重置触发时间
            trigger_flags_cleared = true;
            if(file_logger) {
                 std::string log_msg = "[复位] 已清除 IO " + std::to_string(io.io_index) + " (描述: " + io.description + ") 的内部触发标志.";
                 SPDLOG_INFO(log_msg);
            }
        }
    }

    if (!trigger_flags_cleared) {
        if(file_logger) SPDLOG_INFO("[复位] 调用 resetSpeed 时没有内部触发标志被设置.");
    }

    // 步骤 2: 立即检查所有已配置 IO 的 *当前* 物理状态.
    // 如果 *没有* IO 当前满足其触发条件，则启动恢复.
    // 这防止了在安全条件仍然物理存在时立即恢复.
    bool any_io_currently_meets_trigger = false;
    int still_triggered_io_index = -1; // 用于记录哪个仍然触发
    std::string still_triggered_io_desc; // 用于记录描述
    bool still_triggered_io_current_value = false;
    int still_triggered_io_trigger_value = -1;

    for (const auto& io : io_list_indexed) {
        if (io.is_configured) {
            bool current_value = read_io(io.io_index);
            bool meets_trigger_condition = (current_value == (io.trigger_value == 1));
            if (meets_trigger_condition) {
                any_io_currently_meets_trigger = true;
                still_triggered_io_index = io.io_index;
                still_triggered_io_desc = io.description;
                still_triggered_io_current_value = current_value;
                still_triggered_io_trigger_value = io.trigger_value;

                if(file_logger) {
                    std::string warn_msg = "[复位] IO " + std::to_string(io.io_index) + " (描述: " + io.description +
                                           ") 仍然满足其触发条件 (当前值 " + (current_value ? "1" : "0") +
                                           " == 触发值 " + std::to_string(io.trigger_value) + "), 无法恢复.";
                    SPDLOG_WARN(warn_msg);
                }
                break; // 找到一个活动的触发，无需检查其他
            }
        }
    }

    // 步骤 3: 如果没有 IO 当前触发，尝试转换为 NORMAL 并恢复.
    if (!any_io_currently_meets_trigger) {
        if (was_limited) {
             // 即使监测线程还没跟上，我们也强制进行转换并立即执行动作. 监测线程会同步.
             // 只在我们系统确实处于 LIMITED 状态时这样做.
             if (current_system_state.load(std::memory_order_acquire) == SYSTEM_STATE_LIMITED) { // 在锁下再次检查
                  current_system_state.store(SYSTEM_STATE_NORMAL, std::memory_order_release);
                  if(file_logger) SPDLOG_INFO("[复位] 所有安全条件当前均已解除，启动机器人恢复.");
                  resume_robots(); // 状态转换动作 (在锁定区域内)
             } else {
                  if(file_logger) SPDLOG_INFO("[复位] 系统已处于正常状态，内部标志已清除.");
             }
        } else {
             if(file_logger) SPDLOG_INFO("[复位] 系统已处于正常状态，内部标志已清除.");
        }
        // 成功: 触发标志已清除，恢复已尝试/无需恢复，因为物理条件安全
        return true;
    } else {
        // 如果有 IO 仍然物理触发，系统状态保持 LIMITED
        // (或者如果在 already_triggered 被清除后物理触发仍然存在，监测线程会在下一周期将其转回 LIMITED).
        // 我们不恢复机器人.
        if(file_logger) {
             std::string warn_msg = "[复位] 收到外部复位请求，但安全条件仍在 IO " + std::to_string(still_triggered_io_index) + " (描述: " + still_triggered_io_desc + ") 上激活. 无法恢复机器人.";
             SPDLOG_WARN(warn_msg);
        }
        // 向 HMI/用户发送错误报告
        std::string alert_msg = "外部安全复位命令接收，但安全IO[" + std::to_string(still_triggered_io_index) + "]仍处于触发状态，无法恢复运行.";
        NRC_TriggerErrorReport(2, alert_msg);

        // 为当前活动的物理触发重新设置 already_triggered 标志
        // 这确保监测线程根据当前物理状态将系统保持在 LIMITED 状态
        {
             // 无需再次锁定，我们已经在 lock_guard 中
             for (auto& io : io_list_indexed) {
                 if (io.is_configured) {
                     bool current_value = read_io(io.io_index);
                     bool meets_trigger_condition = (current_value == (io.trigger_value == 1));
                     if (meets_trigger_condition && !io.already_triggered) {
                          io.already_triggered = true;
                          io.trigger_time = std::time(nullptr);
                          if(file_logger) {
                              std::string log_msg = "[复位] IO " + std::to_string(io.io_index) + " 仍然物理触发，重新设置 already_triggered 标志.";
                              SPDLOG_WARN(log_msg);
                          }
                     }
                 }
            }
            // 如果系统状态不是 LIMITED，确保其反映物理实际
            if (current_system_state.load(std::memory_order_acquire) != SYSTEM_STATE_LIMITED) {
                 current_system_state.store(SYSTEM_STATE_LIMITED, std::memory_order_release);
                 // 这里无需再次调用 pause_robots，因为在转换为 LIMITED 时已经调用过
                 // 监测线程会确保状态一致性.
            }
        }

        // 失败: 内部触发标志已清除，但安全条件持续存在
        return false;
    }
    // 返回时释放 Mutex
}

bool getCurrentIOStatus(std::vector<bool>& status) {
    // 这返回所有可能的 IO (0-2048) 的 *物理* 状态.
    try {
        status.clear();
        status.resize(2049, false); // 为可能的 IO 0-2048 调整大小

        // 如果 NRC_ReadTcpBoolVar 线程安全，则在互斥锁外读取 IO 是安全的，
        // 并且 IO 索引本身在读取过程中不会被重新配置中. 配置更改受互斥锁保护，因此整体读取是安全的.
        for (int i = 0; i <= 2048; ++i) { // 循环到 2048
             status[i] = read_io(i); // 读取每个可能 IO 的状态
        }

        // if(file_logger) SPDLOG_DEBUG("已获取 2049 个 IO 的当前物理状态."); // 太啰嗦?
        return true;
    } catch (const std::exception& e) {
        std::cerr << "[光栅安全控制] 获取物理 IO 状态出错: " + std::string(e.what()) << std::endl;
        if(file_logger) SPDLOG_ERROR("获取物理 IO 状态出错: {}", e.what());
        return false;
    }
}

int getCurrentLimitedSpeed() {
    // 此函数返回 *配置的* 限速值.
    // 实际动作总是暂停 (速度 0).
    std::lock_guard<std::mutex> lock(io_mutex); // 保护 configured_limited_speed
    if(file_logger) SPDLOG_DEBUG("已获取当前配置的限速: {}%", configured_limited_speed);
    return configured_limited_speed;
}

std::vector<IOState> getTriggeredIOStates() {
    std::lock_guard<std::mutex> lock(io_mutex); // 保护 io_list_indexed
    std::vector<IOState> states;

    // 根据内部 `already_triggered` 标志返回状态
    for (const auto& io : io_list_indexed) {
        if (io.is_configured && io.already_triggered) {
            IOState state;
            state.io_index = io.io_index;
            state.reset_io_index = io.reset_io_index;
            state.is_triggered = io.already_triggered; // 此处应为 true
            state.trigger_time = io.trigger_time;
            state.description = io.description;
            states.push_back(state);
        }
    }

    if(file_logger) SPDLOG_DEBUG("已获取 {} 个当前标记为已触发 (already_triggered=true) 的 IO.", states.size());
    return states;
}

// --- 服务生命周期函数 ---

void rasterSafetyService() {
    // 忽略 SIGPIPE，防止写入关闭的 socket 时崩溃
    signal(SIGPIPE, SIG_IGN);

    // 注册信号处理函数，实现优雅关机
    // 确保在可能记录问题之前设置好信号处理
    if (signal(SIGINT, handle_shutdown_signal) == SIG_ERR) {
        std::cerr << "[光栅安全控制] 警告：注册 SIGINT 信号处理函数失败!" << std::endl;
    } else {
         std::cout << "[光栅安全控制] 已注册 SIGINT 信号处理函数." << std::endl;
    }
    if (signal(SIGTERM, handle_shutdown_signal) == SIG_ERR) {
        std::cerr << "[光栅安全控制] 警告：注册 SIGTERM 信号处理函数失败!" << std::endl;
    } else {
         std::cout << "[光栅安全控制] 已注册 SIGTERM 信号处理函数." << std::endl;
    }

    // 日志目录和日志系统初始化
    bool log_init_success = true;
    if (!createDirectory(CONFIG_DIR)) { // createDirectory 自身会记录错误
        std::cerr << "[光栅安全控制] 初始化日志目录失败，可能无法写入日志文件." << std::endl;
        log_init_success = false;
    }

    if (log_init_success) {
        std::string log_path = CONFIG_DIR + "/raster_safety.log";
        std::shared_ptr<spdlog::sinks::stdout_color_sink_mt> console_sink;
        std::shared_ptr<spdlog::sinks::rotating_file_sink_mt> file_sink;
        console_sink = std::make_shared<spdlog::sinks::stdout_color_sink_mt>();
        try {
            file_sink = std::make_shared<spdlog::sinks::rotating_file_sink_mt>(
                log_path, LOG_FILE_SIZE, LOG_FILES_COUNT
            );
        } catch (const spdlog::spdlog_ex& ex) {
            std::cerr << "[光栅安全控制] 日志文件初始化失败: " + std::string(ex.what()) << std::endl;
            // 如果日志器创建失败，SPDLOG_ERROR 尚不可用
            log_init_success = false;
        }
        if (log_init_success && file_sink) {
            std::vector<spdlog::sink_ptr> sinks{console_sink, file_sink};
            file_logger = std::make_shared<spdlog::logger>("raster_safety", sinks.begin(), sinks.end());
            spdlog::register_logger(file_logger);
            file_logger->set_pattern("[%Y-%m-%d %H:%M:%S.%e] [%^%l%$] %v");
            file_logger->flush_on(spdlog::level::info); // INFO 及以上立即刷新
            spdlog::set_default_logger(file_logger);
            SPDLOG_INFO("光栅安全控制系统启动. 日志已初始化.");
        } else {
             // 如果文件 sink 失败，回退到仅控制台日志
             spdlog::set_default_logger(spdlog::stdout_color_mt("console_only"));
             spdlog::set_pattern("[%Y-%m-%d %H:%M:%S.%e] [%^%l%$] %v");
             SPDLOG_WARN("光栅安全控制系统启动. 文件日志初始化失败，仅使用控制台日志.");
        }
    } else {
        // 如果目录创建失败，日志可能存在问题. 确保有一些输出.
        std::cerr << "[光栅安全控制] 由于目录问题，文件日志未能完全初始化." << std::endl;
    }

    // 加载配置
    // 首次加载在此处发生. 后续更新通过 updateIOConfig.
    {
        std::lock_guard<std::mutex> lock(io_mutex); // 保护配置加载
        if (!load_from_file()) { // load_from_file 自身会记录错误
            std::cerr << "[光栅安全控制] 启动时配置文件读写存在问题." << std::endl;
            // 继续使用 io_list_indexed 的默认构造函数加载的默认/空配置
        } else {
             std::cout << "[光栅安全控制] 配置文件加载成功." << std::endl;
             if(file_logger) SPDLOG_INFO("配置文件加载成功.");
        }
    }


    // 在监测线程启动前，初始化受控机器人的 robot_states map 条目
    {
        std::lock_guard<std::mutex> lock(io_mutex);
        for(int id : handled_robot_ids) {
           getRobotState(id); // 确保 map 条目存在并获取初始状态
        }
    }


    // 启动监测线程
    thread_running = true;
    // 确保在日志设置完成后创建线程
    monitor_thread = new std::thread(io_monitor_thread);
    std::cout << "光栅安全控制线程启动成功" << std::endl;
    if(file_logger) SPDLOG_INFO("光栅安全控制监测线程启动.");


    // 服务主循环 - 使主线程保持活动直到关机
    if(file_logger) SPDLOG_INFO("[光栅安全控制] rasterSafetyService 主循环正在运行，等待停止信号.");
    while (thread_running) {
        // 核心逻辑在 io_monitor_thread 中.
        // 此循环仅保持主线程活动.
        std::this_thread::sleep_for(std::chrono::milliseconds(500));
    }
    if(file_logger) SPDLOG_INFO("[光栅安全控制] rasterSafetyService 主循环退出.");
}

void stopRasterSafetyService() {
    // 确保在关机序列期间可以写入日志
    if (file_logger) {
        SPDLOG_INFO("[光栅安全控制] 收到停止服务请求...");
    } else {
        std::cerr << "[光栅安全控制] 收到停止服务请求..." << std::endl;
    }

    // 1. 通知监测线程停止
    thread_running = false; // 设置原子标志

    // 2. 等待监测线程完成当前循环并退出
    if (monitor_thread != nullptr && monitor_thread->joinable()) {
        if (file_logger) {
             SPDLOG_INFO("[光栅安全控制] 等待 IO 监测线程结束...");
        } else {
             std::cerr << "[光栅安全控制] 等待 IO 监测线程结束..." << std::endl;
        }
        // 如果监测线程可能持有互斥锁，尝试在 join *之前* 获取锁可能会有帮助
        // 尽管设置 thread_running=false 最终会使其退出循环，即使它处于 sleep 状态.
        // 在复杂场景中，更好的方法是使用条件变量或中断机制.
        // 对于这个简单的 sleep 循环，直接 join 应该可行.

        monitor_thread->join(); // 阻塞直到线程函数返回
        if (file_logger) {
            SPDLOG_INFO("[光栅安全控制] IO 监测线程已结束.");
        } else {
             std::cerr << "[光栅安全控制] IO 监测线程已结束." << std::endl;
        }

        // 3. 清理线程对象
        delete monitor_thread;
        monitor_thread = nullptr; // 防止二次删除
    } else {
         if (file_logger) {
             SPDLOG_WARN("[光栅安全控制] IO 监测线程不可 join 或未运行.");
         } else {
             std::cerr << "[光栅安全控制] IO 监测线程不可 join 或未运行." << std::endl;
         }
    }

    // --- 其他清理任务 (如果有) ---
    // Spdlog 清理 (可选，通常在退出时自动发生)
     if (file_logger) {
        file_logger->flush(); // 确保任何缓冲的日志已写入
     }
    // spdlog::shutdown(); // 如果需要强制刷新/清理则调用

    if (file_logger) {
        SPDLOG_INFO("光栅安全控制服务已停止.");
    } else {
         std::cerr << "光栅安全控制服务已停止." << std::endl;
    }
}

// --- 信号处理函数 ---
static void handle_shutdown_signal(int signal_num) {
    // 简单的处理函数，请求关机.
    // 最好只在此处设置一个原子标志，让主循环处理实际关机.
    std::cerr << "[光栅安全控制] 信号处理函数收到信号: " + std::to_string(signal_num) + ", 请求服务停止." << std::endl;
    thread_running = false; // 设置原子标志是信号安全的.
    // 主循环 (rasterSafetyService) 将检测到 thread_running 为 false 并退出，
    // 然后调用 stopRasterSafetyService 来 join 监测线程.
}


// --- 请求处理函数 (来自 NRC Socket 回调) ---
void rasterSafetyControl(const Json::Value &root) {
    if (!root.isMember("reqRasterSafetyControl")) {
        // 不是我们的请求类型或格式错误
        return;
    }
    const Json::Value& request = root["reqRasterSafetyControl"];

    if (!request.isMember("operation") || !request["operation"].isString()) {
        std::cerr << "[光栅安全控制] 无效的请求格式: 缺少或operation字段无效" << std::endl;
        if(file_logger) SPDLOG_ERROR("无效的请求格式: 缺少或operation字段无效");
        // 发送负面响应
        Json::Value response;
        response["reqRasterSafetyControlCB"] = Json::Value(Json::objectValue);
        response["reqRasterSafetyControlCB"]["status"] = false;
        response["reqRasterSafetyControlCB"]["message"] = "无效请求: 缺少或operation无效";
        NRC_SendSocketCustomProtocal(0x927b, Json::FastWriter().write(response));
        return;
    }

    std::string operation = request["operation"].asString();
    Json::Value response;
    response["reqRasterSafetyControlCB"] = Json::Value(Json::objectValue);
    response["reqRasterSafetyControlCB"]["operation"] = operation; // 回显操作

    if(file_logger) SPDLOG_INFO("收到光栅安全控制请求操作: {}", operation);

    if (operation == "update_config") {
        if (!request.isMember("limited_speed") || !request["limited_speed"].isInt() ||
            !request.isMember("config_data") || !request["config_data"].isArray())
        {
            if(file_logger) SPDLOG_WARN("更新配置: 缺少必要参数 (limited_speed 或 config_data) 或类型错误.");
            response["reqRasterSafetyControlCB"]["status"] = false;
            response["reqRasterSafetyControlCB"]["message"] = "缺少或无效参数";
        } else {
            int limited_speed = request["limited_speed"].asInt();
            const Json::Value& config_data_json = request["config_data"];

            std::vector<IOConfig> new_config_vec;
            // 从 Json::Value 解析到 vector<IOConfig> 并验证
            for (const auto& item : config_data_json) {
                 if (!item.isObject()) {
                      if(file_logger) SPDLOG_WARN("更新配置: config_data 数组中无效条目 (不是对象). 跳过.");
                      continue;
                 }
                 if (!item.isMember("io_index") || !item["io_index"].isInt()) {
                      if(file_logger) SPDLOG_WARN("更新配置: config_data 数组中无效条目 (缺少或无效 io_index). 跳过.");
                      continue;
                 }

                 int io_index = item["io_index"].asInt();
                 int reset_io_index = item.get("reset_io_index", 0).asInt(); // 默认 0
                 int trigger_value_int = item.get("trigger_value", 1).asInt(); // 默认 1 (高电平)
                 std::string description = item.get("description", "").asString();

                 // 验证基本范围
                 if (io_index < 0 || io_index > 2048) {
                     if(file_logger) SPDLOG_WARN("更新配置: 配置条目中无效的 io_index {}. 跳过.", io_index);
                     continue; // 跳过无效 io_index
                 }
                 if (reset_io_index < 0 || reset_io_index > 2048) {
                      if(file_logger) SPDLOG_WARN("更新配置: IO {} 的复位 io_index {} 无效. 为此条目将 reset_io_index 设为 0.", io_index, reset_io_index);
                      reset_io_index = 0; // 修正无效的复位索引
                 }
                 if (trigger_value_int != 0 && trigger_value_int != 1) {
                      if(file_logger) SPDLOG_WARN("更新配置: IO {} 的 trigger_value {} 无效. 使用默认值 1.", io_index, trigger_value_int);
                      trigger_value_int = 1; // 修正无效的触发值
                 }


                 // 创建 IOConfig 并添加到向量
                 // IOConfig 构造函数接受 int，但内部使用 bool 进行 trigger_value 比较
                 IOConfig config(io_index, reset_io_index, trigger_value_int, description);
                 new_config_vec.push_back(config);
            }

            // 调用内部更新函数，传入验证后的向量
            bool success = updateIOConfig(new_config_vec, limited_speed);
            response["reqRasterSafetyControlCB"]["status"] = success;
            response["reqRasterSafetyControlCB"]["message"] = success ? "配置已更新" : "配置更新失败 (请查看日志)";
        }

    } else if (operation == "reset_speed") { // 这意味着 "重置触发并尝试恢复"
        // 此操作在请求 JSON 中不需要额外参数
        bool success = resetSpeed(); // 调用重构后的复位函数
        response["reqRasterSafetyControlCB"]["status"] = success;
        response["reqRasterSafetyControlCB"]["message"] = success ? "触发已重置，已尝试恢复" : "触发已重置，但安全条件仍然激活. 恢复失败.";
        response["reqRasterSafetyControlCB"]["limited_speed"] = getCurrentLimitedSpeed(); // 返回配置的限速值
        // 注意: 如果仍然受限，实际速度是 0；如果成功，则恢复到正常运行速度.

    } else if (operation == "get_config") {
        // 此操作在请求 JSON 中不需要额外参数
        response["reqRasterSafetyControlCB"]["status"] = true; // 除非获取数据出错，否则假定成功

        response["reqRasterSafetyControlCB"]["limited_speed"] = getCurrentLimitedSpeed(); // 获取配置的限速值

        Json::Value config_data_array(Json::arrayValue);
        std::vector<IOState> triggered_states = getTriggeredIOStates(); // 获取当前已触发的状态 (获取内部锁)

        // 访问 io_list_indexed 获取所有已配置的 IO (需要锁)
        std::lock_guard<std::mutex> lock(io_mutex);
        for (int i = 0; i < io_list_indexed.size(); ++i) {
            const auto& io = io_list_indexed[i];
            if (io.is_configured) {
                Json::Value item;
                item["io_index"] = io.io_index;
                item["trigger_value"] = io.trigger_value; // 返回存储的 int 值 (0 或 1)
                item["reset_io_index"] = io.reset_io_index;
                item["description"] = io.description;

                // 检查此已配置 IO 是否在当前已触发状态列表中
                bool is_currently_triggered = false;
                for(const auto& triggered_io : triggered_states) {
                    if (triggered_io.io_index == io.io_index) {
                        is_currently_triggered = true;
                        break;
                    }
                }
                item["is_triggered"] = is_currently_triggered; // 添加当前触发状态

                config_data_array.append(item);
            }
        }
        response["reqRasterSafetyControlCB"]["config_data"] = config_data_array;
        // 除非发生错误，否则 get_config 成功时没有特定的 message.
        // 如果 getTriggeredIOStates或 getCurrentLimitedSpeed 失败，status 会是 false.

    } else {
        std::cerr << "[光栅安全控制] 未知的操作类型: " + operation << std::endl;
        if(file_logger) SPDLOG_WARN("收到未知的操作类型: {}", operation);
        response["reqRasterSafetyControlCB"]["status"] = false;
        response["reqRasterSafetyControlCB"]["message"] = "未知操作";
    }

    // 使用 JsonCpp 的 FastWriter 发送响应
    NRC_SendSocketCustomProtocal(0x927b, Json::FastWriter().write(response));
}
