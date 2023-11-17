#define DUCKDB_EXTENSION_MAIN

#include "oml_reader_extension.hpp"
#include "duckdb.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/main/extension_util.hpp"
#include <duckdb/parser/parsed_data/create_scalar_function_info.hpp>
#include <duckdb/parser/parsed_data/create_table_function_info.hpp>
#include <fstream>
// #include <sstream>
// #include <vector>
#include <iostream>


#include "oml_parser.cpp"
#include "oml_parser.hpp"



namespace duckdb
{
    inline void OMLScanInternal(ClientContext &context, TableFunctionInput &data_p, DataChunk &output){
        auto &global_data = data_p.global_state->Cast<GlobalState>();
        auto &bind_data = data_p.bind_data->Cast<BindState>();
        idx_t actual_row = 0;

        for (size_t i = global_data.rows_read; i < bind_data.fileRows.size(); ++i) {
            if (actual_row >= STANDARD_VECTOR_SIZE ){
                break;
            }
            std::istringstream iss(bind_data.fileRows[i]);
            std::vector<std::string> tokens;

            // break the row into a list of numbers
            do {
                std::string token;
                iss >> token;
                if (!token.empty()) {
                    tokens.push_back(token);
                }
            } while (iss);

            
            // Going over the row values (their idx (j) corresponds to the column idx)
            for (size_t j = 0; j < tokens.size(); j++) {
                duckdb::Value duckVal(tokens.at(j));
                output.SetValue(j, actual_row, duckVal);
            }
            actual_row ++; 
            global_data.rows_read++;
        }
        output.SetCardinality(actual_row);  
    }


    // Bind Function for Table creation 
    unique_ptr<FunctionData> OMLScanBind(ClientContext &context, TableFunctionBindInput &input, vector<LogicalType> &return_types, vector<string> &names){
        auto file = input.inputs[0].ToString();
        auto result = make_uniq<BindState>();

        OMLParser parser(return_types, names, file);
        return_types = parser.GetReturnTypes();
        names = parser.GetNames();
        result->fileRows =  parser.GetRows();
        return std::move(result);
    }

    // Setup GLobal State 
    unique_ptr<GlobalTableFunctionState> OMLScanInitGlobal(ClientContext &context, TableFunctionInitInput &input){
        auto result = make_uniq<GlobalState>();
        // auto bind_data = input.bind_data->Cast<BindState>();
        result->rows_read = 0;
        return std::move(result);
    }




    inline void OMLScalarFunInternal(DataChunk &args, ExpressionState &state, Vector &result) {
        auto &name_vector = args.data[0];
        UnaryExecutor::Execute<string_t, string_t>(
            name_vector, result, args.size(),
            [&](string_t name) {
                return StringVector::AddString(result, "OML "+name.GetString()+" 🐥");;
            });
    }

    
    static void LoadInternal(DatabaseInstance &instance)
    {   
        // Register all table functions
        //  OMLScanBind -- Used to create the table Schema based on the metadata of the input file. The reading is done by the Oml Parser class.
        //  OMLScanInternal -- Loads the tuples (rows) of the file into the already defined table.
        auto oml_reader_fun= TableFunction("oml_reader", {LogicalType::VARCHAR}, OMLScanInternal ,OMLScanBind,OMLScanInitGlobal);
        ExtensionUtil::RegisterFunction(instance, oml_reader_fun);


        auto oml_reader_scalar= ScalarFunction("oml", {LogicalType::VARCHAR}, LogicalType::VARCHAR, OMLScalarFunInternal);
        ExtensionUtil::RegisterFunction(instance, oml_reader_scalar);
    }

    void OmlReaderExtension::Load(DuckDB &db)
    {
        LoadInternal(*db.instance);
    }
    std::string OmlReaderExtension::Name()
    {
        return "oml_reader";
    }

} // namespace duckdb

extern "C"
{

    DUCKDB_EXTENSION_API void oml_reader_init(duckdb::DatabaseInstance &db)
    {
        LoadInternal(db);
        
    }

    DUCKDB_EXTENSION_API const char *oml_reader_version()
    {
        return duckdb::DuckDB::LibraryVersion();
    }
}

#ifndef DUCKDB_EXTENSION_MAIN
#error DUCKDB_EXTENSION_MAIN not defined
#endif