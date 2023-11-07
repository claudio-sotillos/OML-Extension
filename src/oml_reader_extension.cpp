#define DUCKDB_EXTENSION_MAIN

#include "oml_reader_extension.hpp"
#include "duckdb.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/main/extension_util.hpp"
#include <duckdb/parser/parsed_data/create_scalar_function_info.hpp>

#include <fstream>
#include <sstream>
#include <vector>
#include <iostream>
#include <string>
#include <map>
#include <unordered_map> 

namespace duckdb
{


    std::map<std::string, std::vector<std::string>> ParseFile(const std::string& filePath) {
        std::map<std::string, std::vector<std::string>> dataMap;
        std::ifstream file(filePath);
        std::string line;
        int schemaRead = 0;
        int colIdx = 0;
        std::unordered_map<int,std::string> hashmap;

        // Skip lines until a line with "schema:" is found
        while (std::getline(file, line)) {

            if (line.find("schema:") != std::string::npos) {
                
                std::istringstream schemaLine(line);
                std::string token;
                while (schemaLine >> token) {
                    if (token.find(":") != std::string::npos && token != "schema:" ) {
                        size_t pos = token.find(":");

                        
                        dataMap[token.substr(0, pos)] = std::vector<std::string>();
                        
                        hashmap[colIdx] = token.substr(0, pos);
                        colIdx  += 1; 

                    }
                }

                schemaRead += 1; 
            }

            if (schemaRead == 2) {
                break;
            }
        }   
    
        // Skip the next two lines
        
        std::getline(file, line);
        std::getline(file, line);

        // Read and parse data lines
        while (std::getline(file, line)) {
            std::istringstream iss(line);
            std::string token;
            colIdx = 0;
            while (iss >> token) {
                dataMap[hashmap[colIdx]].push_back(token);
                colIdx += 1;
            }
        }

        file.close();
        return dataMap;
    }

    inline void OmlReaderScalarFun(DataChunk &args, ExpressionState &state, Vector &result) {
        auto &name_vector = args.data[0];
        UnaryExecutor::Execute<string_t, std::vector<duckdb::string_t>>(
            name_vector, result, args.size(),
            [&](string_t filename) {
                std::map<std::string, std::vector<std::string>> dataMap = ParseFile(filename.GetString());
                std::vector<duckdb::string_t> firstValues;

                // Check if the map is not empty and the first column has values
                if (!dataMap.empty() && !dataMap.begin()->second.empty()) {
                    std::vector<std::string>& firstColumn = dataMap.begin()->second;
                    for (const auto& value : firstColumn) {
                        firstValues.emplace_back(value);
                    }
                }

                std::cout << "Vector size: " << firstValues.size() << std::endl;   
                // for (int i = 0; i < 5 && i < firstValues.size(); ++i) {
                //         std::cout << firstValues[i] << std::endl;
                //     }

                return firstValues;            

                // // Set the result vector to store string_t values
                // result.Initialize(firstValues.size(), LogicalType::VARCHAR);
                // for (size_t i = 0; i < firstValues.size(); ++i) {
                //     result.SetValue(i, firstValues[i].GetDataUnsafe(), firstValues[i].GetSize());
                // }
            });
    }




    // inline void OmlReaderScalarFun(DataChunk &args, ExpressionState &state, Vector &result) {
    //     auto &name_vector = args.data[0];
    //     UnaryExecutor::Execute<string_t, string_t>(
    //         name_vector, result, args.size(),
    //         [&](string_t filename) {
    //             std::ifstream file(filename.GetString());
    //             std::string line;
    //             std::map<std::string, std::vector<std::string>> dataMap;
    //             if (file.is_open()) {
    //                 if (std::getline(file, line)) {
    //                     std::cout << "First line of the file: " << line << std::endl;
    //                     return StringVector::AddString(result, line);
    //                 }
    //                 file.close();
    //             }
    //             // Return an empty string if the file couldn't be opened or read
    //             return StringVector::AddString(result, "");
    //         });
    // }

    static void LoadInternal(DatabaseInstance &instance)
    {
        // Register a scalar function
        // auto oml_reader_scalar_function = ScalarFunction("oml_reader", {LogicalType::VARCHAR}, LogicalType::VARCHAR, OmlReaderScalarFun);
        // ExtensionUtil::RegisterFunction(instance, oml_reader_scalar_function);
        auto oml_reader_scalar_function = TableFunction("oml_reader", {LogicalType::VARCHAR}, OmlReaderScalarFun);
        ExtensionUtil::RegisterFunction(instance, oml_reader_scalar_function);
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
