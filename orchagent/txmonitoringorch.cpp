#include "txmonitoringorch.h"
#include "logger.h"
#include "sai_serialize.h"
#include "converter.h"
#include "portsorch.h"

extern sai_port_api_t *sai_port_api;
extern PortsOrch*       gPortsOrch;

#define TX_MONITORING_TABLE "TX_MONITOR_CONFIG_TABLE"
#define TX_MONITORING_STATUS_TABLE "TX_MONITOR_STATUS_TABLE"
#define COUNTERS_TABLE "COUNTERS"
#define DEFAULT_TIME_PERIOD 10
#define TX_MONITORING_THRESHOLD "tx_monitoring_threshold"
#define TX_MONITORING_TIME_PERIOD "tx_monitoring_time_period"
#define TX_MONITORING_CONFIG_KEY "tx_monitoring_config"
#define TX_MONITORING_STATUS_KEY "tx_monitoring_status"
#define TX_COUNTERS_FIELD "SAI_PORT_STAT_IF_OUT_ERRORS"

TxMonitorOrch::TxMonitorOrch(swss::DBConnector *db, const std::string &table_name):
    Orch(db, table_name)
{
    SWSS_LOG_ENTER();
    // Initialize DB connectors
    m_counters_db = std::make_shared<swss::DBConnector>("COUNTERS_DB", 0);
    m_state_db = std::make_shared<swss::DBConnector>("STATE_DB", 0);
    m_counters_table = std::make_shared<swss::Table>(m_counters_db.get(), COUNTERS_TABLE);
    m_state_table = std::make_shared<swss::Table>(m_state_db.get(), TX_MONITORING_STATUS_TABLE);

    // Initialize timer for periodic monitoring
    auto interv = timespec { .tv_sec = DEFAULT_TIME_PERIOD, .tv_nsec = 0 }; // Default 10 second, will be updated from CONFIG_DB
    m_timer = new swss::SelectableTimer(interv);
    auto executor = new swss::ExecutableTimer(m_timer, this, "TX_MONITORING_TIMER");
    Orch::addExecutor(executor);
    m_timer->start();

    // Initialize default values
    m_threshold = 1;
    m_time_period = DEFAULT_TIME_PERIOD;

    // Initialize tx monitoring config table
    m_tx_monitoring_config_table = std::make_shared<swss::Table>(db, table_name);
    init_tx_monitoring_config();
}

void TxMonitorOrch::init_tx_monitoring_config()
{
    // This function initialize the tx monitoring config table with default values
    SWSS_LOG_ENTER();
    vector<FieldValueTuple> fvs;
    fvs.emplace_back(TX_MONITORING_THRESHOLD, to_string(m_threshold));
    fvs.emplace_back(TX_MONITORING_TIME_PERIOD, to_string(m_time_period));
    m_tx_monitoring_config_table->set(TX_MONITORING_CONFIG_KEY, fvs);
}

TxMonitorOrch::~TxMonitorOrch(void)
{
    SWSS_LOG_ENTER();
}

void TxMonitorOrch::doTask(Consumer &consumer)
{
    // This function handles the tx monitoring config table updates
    SWSS_LOG_ENTER();
    try 
    {
        auto it = consumer.m_toSync.begin();
        while (it != consumer.m_toSync.end())
        {
            KeyOpFieldsValuesTuple t = it->second;
            string key = kfvKey(t);
            string op = kfvOp(t);
            
            SWSS_LOG_INFO("Processing task - key: %s, operation: %s", key.c_str(), op.c_str());

            if (op == SET_COMMAND)
            {
                vector<FieldValueTuple> fvs = kfvFieldsValues(t);
                for (auto fv : fvs)
                {
                    if (!fvValue(fv).empty() && stoi(fvValue(fv)) < 1)
                    {
                        SWSS_LOG_ERROR("Invalid value: %s, should be a positive number", fvValue(fv).c_str());
                        continue;
                    }
                    if (fvField(fv) == TX_MONITORING_THRESHOLD)
                    {
                        m_threshold = to_uint<uint32_t>(fvValue(fv));
                        SWSS_LOG_INFO("Updated tx_monitoring_threshold to %u", m_threshold);
                    }
                    else if (fvField(fv) == TX_MONITORING_TIME_PERIOD)
                    {
                        m_time_period = to_uint<uint32_t>(fvValue(fv));
                        auto interv = timespec { .tv_sec = m_time_period, .tv_nsec = 0 };
                        m_timer->setInterval(interv);
                        m_timer->reset();
                        SWSS_LOG_INFO("Updated tx_monitoring_time_period to %u seconds", m_time_period);
                    }
                }
            }
            it = consumer.m_toSync.erase(it);
        }
    }
    catch (const std::exception& e)
    {
        SWSS_LOG_ERROR("Error processing task: %s", e.what());
    }
}

void TxMonitorOrch::doTask(swss::SelectableTimer &timer)
{
    // This function called periodically to collect the tx counters and update the port status in state_db
    SWSS_LOG_ENTER();
    try
    {
        if (timer.getFd() == m_timer->getFd())
        {
            collect_tx_counters(m_port_status);
            update_ports_tx_status(m_port_status);
        }
    }
    catch (const std::exception& e)
    {
        SWSS_LOG_ERROR("Error in timer task: %s", e.what());
    }
}

void TxMonitorOrch::collect_tx_counters(std::unordered_map<std::string, std::string>& port_status)
{
    // This function collect the tx counters from the counters_db and update the port status in state_db
    SWSS_LOG_ENTER();
    try
    {
        for (const auto &port : gPortsOrch->getAllPorts())
        {
            string portName = port.first;
            if (portName == "CPU")
            {
                continue;
            }
            string port_oid = sai_serialize_object_id(port.second.m_port_id);

            SWSS_LOG_INFO("Collecting counters for port %s", portName.c_str());

            string value;
            m_counters_table->hget(port_oid, TX_COUNTERS_FIELD, value);
            if (!value.empty())
            {
                uint64_t currentTxErrors = to_uint<uint64_t>(value);
                uint64_t prevTxErrors = m_prev_tx_counters[portName];
                m_prev_tx_counters[portName] = currentTxErrors;
                uint64_t txErrorsDiff = currentTxErrors - prevTxErrors;
                m_port_status[portName] = (txErrorsDiff >= m_threshold) ? "Not OK" : "OK";
            }
            else
            {
                SWSS_LOG_WARN("Failed to get counter value for port %s", portName.c_str());
            }
        }
    }
    catch (const std::exception& e)
    {
        SWSS_LOG_ERROR("Error collecting TX counters: %s", e.what());
    }
}

void TxMonitorOrch::update_ports_tx_status(std::unordered_map<std::string, std::string>& port_status)
{
    // This function update the port status in state_db
    SWSS_LOG_ENTER();
    try
    {
        for (const auto &port_status : m_port_status)
        {
            string key = port_status.first;
            string status = port_status.second;
            vector<FieldValueTuple> fvs;
            fvs.emplace_back(TX_MONITORING_STATUS_KEY, status);
            m_state_table->set(key, fvs);
            SWSS_LOG_INFO("Updated status for port %s to %s", key.c_str(), status.c_str());
        }
    }
    catch (const std::exception& e)
    {
        SWSS_LOG_ERROR("Error updating port status: %s", e.what());
    }
}
