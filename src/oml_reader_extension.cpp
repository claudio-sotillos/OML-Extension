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
#include <sstream>
#include <vector>
#include <iostream>
#include <string>
#include <filesystem>


#include "oml_parser.cpp"
#include "oml_parser.hpp"



namespace duckdb
{


    inline void OMLScanInternal(ClientContext &context, TableFunctionInput &data_p, DataChunk &output){
        // this is the last call
        auto &global_data = data_p.global_state->Cast<OMLGlobalState>();
        auto &bind_data = data_p.bind_data->Cast<OmlBindState>();
        idx_t actual_row = 0;


        // std::vector<duckdb::Value> duck_row{};

        for (size_t i = global_data.rows_read; i < bind_data.fileRows.size(); ++i) {
            if (actual_row >= STANDARD_VECTOR_SIZE ){
                break;
            }
            std::istringstream iss(bind_data.fileRows[i]);
            std::vector<std::string> tokens;

            // break the line into a list of the numbers
            do {
                std::string token;
                iss >> token;
                if (!token.empty()) {
                    tokens.push_back(token);
                }
            } while (iss);

            
            // std::ofstream outFile("output.txt");
            for (size_t j = 0; j < tokens.size(); j++) {
                // outFile << tokens.at(j) << std::endl;
                duckdb::Value dVal(tokens.at(j));
                output.SetValue(j, actual_row, dVal);
            }
            // outFile.close();
            actual_row ++; 
            global_data.rows_read++;
        }

        // while(rows_read_local < STANDARD_VECTOR_SIZE && bind_data.reader_bind->ReadRow(global_data.rows_read, duck_row)){
        //     if (duck_row.empty()){break;}

        //     for (long unsigned int i = 0; i < duck_row.size(); i++) {
        //         auto v = duck_row.at(i);
        //         output.SetValue(i, rows_read_local, v);
        //     }

        //     global_data.rows_read++;
        //     rows_read_local++;
        //     duck_row = std::vector<duckdb::Value>();
        // }

        // if (rows_read_local == 0)
        // {
        //     return;
        // }
        
        output.SetCardinality(actual_row);  
        global_data.chunk_count += 1;
    }


    // Bind inputs for Custom_Power_load table function
    unique_ptr<FunctionData> OMLScanBind(ClientContext &context, TableFunctionBindInput &input, vector<LogicalType> &return_types, vector<string> &names){
        
        auto file = input.inputs[0].ToString();
        auto result = make_uniq<OmlBindState>();

        result->file = file;

        OMLParser parser(return_types, names, file);
        return_types = parser.GetReturnTypes();
        names = parser.GetNames();

        // result->reader_bind = make_shared<OMLParser>(return_types, names, file);
        result->fileRows =  parser.GetRows();
        

        return std::move(result);
    }

    unique_ptr<GlobalTableFunctionState> OMLScanInitGlobal(ClientContext &context, TableFunctionInitInput &input){
        // set the global state
        auto result = make_uniq<OMLGlobalState>();
        auto bind_data = input.bind_data->Cast<OmlBindState>();

        // create a reader
        //result->reader_bind = OMLReader(bind_data.return_types, bind_data.names, bind_data.file);
        result->rows_read = 0;
        
        return std::move(result);
    }

    static void LoadInternal(DatabaseInstance &instance)
    {
        // Register a scalar function
        // auto oml_reader_scalar_function = ScalarFunction("oml_reader", {LogicalType::VARCHAR}, LogicalType::VARCHAR, OmlParserScalarFun);
        // ExtensionUtil::RegisterFunction(instance, oml_reader_scalar_function);
        auto oml_reader_fun= TableFunction("oml_reader", {LogicalType::VARCHAR}, OMLScanInternal ,OMLScanBind,OMLScanInitGlobal);
        ExtensionUtil::RegisterFunction(instance, oml_reader_fun);
    }


    // static void LoadInternal(DatabaseInstance &instance) {
    //     // register table functions
    //     for (auto &fun : OMLFunctions::GetScalarFunctions()) {
    //     ExtensionUtil::RegisterFunction(instance, fun);
    //     }

    //     // register table functions
    //     for (auto &fun : OMLFunctions::GetTableFunctions()) {
    //     ExtensionUtil::RegisterFunction(instance, fun);
    //     }
    // }

    // inline void OmlParser(DataChunk &args, ExpressionState &state, Vector &result) {
    //     auto &name_vector = args.data[0];
    //     UnaryExecutor::Execute<string_t, std::vector<duckdb::string_t>>(
    //         name_vector, result, args.size(),
    //         [&](string_t filename) {
    //             auto [dataMap, hashmap] = ParseFile(filename);
    //             std::vector<duckdb::string_t> firstValues;

    //             // Check if the map is not empty and the first column has values
    //             if (!dataMap.empty() && !dataMap.begin()->second.empty()) {
    //                 std::vector<std::string>& firstColumn = dataMap.begin()->second;
    //                 for (const auto& value : firstColumn) {
    //                     firstValues.emplace_back(value);
    //                 }
    //             }
    //             std::cout << "Vector size: " << firstValues.size() << std::endl;   
    //             return firstValues;            
    //         });
    // }


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