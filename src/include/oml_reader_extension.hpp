#pragma once
#include "duckdb.hpp"

namespace duckdb {

class OmlReaderExtension : public Extension {
public:
	void Load(DuckDB &db) override;
	std::string Name() override;
};


struct GlobalState : public GlobalTableFunctionState {
public:
	idx_t rows_read;
};

struct BindState : public TableFunctionData {
public:
	std::vector<std::string> fileRows; // OmlGen only uses this attribute from the bind state  (is also used by Power_Consumption_load)

	// The below attributes are only used by Power_Consumption_load
	std::string schema = "main";
    std::string table =  "Power_Consumption";
	duckdb::unique_ptr<InternalAppender> appender;

};

} // namespace duckdb
