#ifndef TXMONITORINGORCH_H
#define TXMONITORINGORCH_H

#include "orch.h"
#include "port.h"
#include "timer.h"
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

extern "C" {
#include "sai.h"
}

class TxMonitorOrch : public Orch
{
public:
    TxMonitorOrch(swss::DBConnector *db, const std::string &table_name);
    virtual ~TxMonitorOrch(void);

    virtual void doTask(Consumer &consumer);
    virtual void doTask(swss::SelectableTimer &timer);

private:
    void init_tx_monitoring_config();
	void collect_tx_counters(std::unordered_map<std::string, std::string>& port_status);
	void update_ports_tx_status(std::unordered_map<std::string, std::string>& port_status);

    unsigned int m_threshold;
    unsigned int m_time_period;
    std::unordered_map<std::string, uint64_t> m_prev_tx_counters;
    std::unordered_map<std::string, std::string> m_port_status;

    std::shared_ptr<swss::DBConnector> m_counters_db;
    std::shared_ptr<swss::DBConnector> m_state_db;
    std::shared_ptr<swss::Table> m_counters_table;
    std::shared_ptr<swss::Table> m_state_table;
    std::shared_ptr<swss::Table> m_tx_monitoring_config_table;

    swss::SelectableTimer *m_timer;
};

#endif /* TXMONITORINGORCH_H */
