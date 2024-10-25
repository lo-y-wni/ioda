/*
 * (C) Crown copyright 2024, Met Office
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 */

#ifndef CONTAINERS_ICOLSDATA_H_
#define CONTAINERS_ICOLSDATA_H_

#include <cstdint>
#include <memory>
#include <string>
#include <vector>

namespace osdf {
class ColumnMetadata;
class DataBase;

/// \class IColsData
/// This pure abstract class is currently used to enable common operations on data in containers to
/// be handled by a single function. This approach reduces code repetition and enhances
/// maintainability. By passing pointers to an IColsData class into functions, it doesn't care
/// about the type or underlying structure of the data and is able to determine what it needs from
/// the functions this interface implements.
/// \see FunctionsCols::sliceRows

class IColsData {
 public:
  IColsData() {}

  IColsData(IColsData&&)                 = delete;
  IColsData(const IColsData&)            = delete;
  IColsData& operator=(IColsData&&)      = delete;
  IColsData& operator=(const IColsData&) = delete;

  virtual const std::int32_t getSizeCols() const = 0;
  virtual const std::int64_t getSizeRows() const = 0;

  virtual const std::int32_t getIndex(const std::string&) const = 0;

  virtual const std::shared_ptr<DataBase>& getDataColumn(const std::int32_t) const = 0;

  virtual const std::vector<std::int64_t>& getIds() const = 0;
  virtual const ColumnMetadata& getColumnMetadata() const = 0;
  virtual const std::vector<std::shared_ptr<DataBase>>& getDataCols() const = 0;
};
}  // namespace osdf

#endif  // CONTAINERS_ICOLSDATA_H_
