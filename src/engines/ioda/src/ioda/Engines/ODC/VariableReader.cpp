/*
 * (C) Crown copyright 2024, Met Office
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 */

#include "ioda/Engines/ODC/VariableReader.h"
#include "ioda/Engines/ODC/VariableReaderFactory.h"
#include "ioda/Engines/ODC/OdbConstants.h"
#include "ioda/Engines/ODC/DataFromSQL.h"

namespace ioda {
namespace Engines {
namespace ODC {

namespace {

VariableReaderMaker<VariableReaderFromRowsWithNonMissingValues>
readerFromRowsWithNonMissingValuesMaker("from rows with non-missing values");
VariableReaderMaker<VariableReaderFromRowsWithMatchingVarnos>
readerFromRowsWithMatchingVarnosMaker("from rows with matching varnos");

// -----------------------------------------------------------------------------

std::uint64_t bitfieldMask(const std::string &column, const std::string &member,
                           const DataFromSQL &sqlData)
{
  if (sqlData.getColumnTypeByName(column) != odb_type_bitfield)
    throw eckit::BadValue("Column '" + column + "' is not a bitfield", Here());
  int position = 0, size = 0;
  const bool hasMember = sqlData.getBitfieldMemberDefinition(column, member, position, size);
  if (!hasMember)
    throw eckit::BadValue("Bitfield column '" + column + "' has no member '" + member + "'",
                          Here());
  if (size != 1)
    // Technically, this is not supported mainly because multi-bit members would not necessarily
    // fit into an 8-bit char.
    throw eckit::NotImplemented("Loading of bitfield column members composed of multiple bits, "
                                "such as '" + column + "." + member + "', is not supported",
                                Here());
  return static_cast<std::uint64_t>(1) << position;
}

template <typename T>
T convertTo(double x);

template <>
float convertTo<float>(double x) { return static_cast<float>(x); }

template <>
int convertTo<int>(double x) { return static_cast<int>(x); }

template <>
std::string convertTo<std::string>(double ud) {
  // In ODB data is retrieved as doubles but character data is stored as ASCII bits.
  // A reinterpret_cast is used here to re-interpret the retrieved doubles as 8 character chunks.
  char uc[9];
  strncpy(uc, reinterpret_cast<char*>(&ud), 8);
  uc[8] = '\0';

  // This attempts to trim the string.
  std::string s   = std::string(uc);
  size_t endpos   = s.find_last_not_of(" ");
  size_t startpos = s.find_first_not_of(" ");
  if (std::string::npos != endpos) {
    s = s.substr(0, endpos + 1);
    s = s.substr(startpos);
  } else {
    s.erase(std::remove(std::begin(s), std::end(s), ' '), std::end(s));
  }
  return s;
}

}  // namespace

// -----------------------------------------------------------------------------

VariableReaderFromRowsWithNonMissingValues::VariableReaderFromRowsWithNonMissingValues(
    const Parameters_ &, const std::string &column, const std::string &member,
    const DataFromSQL &sqlData)
  : member_(member), sqlData_(sqlData) {
  columnIndex_ = sqlData.getColumnIndex(column);
  if (columnIndex_ >= 0 && !member.empty())
    bitfieldMask_ = bitfieldMask(column, member, sqlData);
}

void VariableReaderFromRowsWithNonMissingValues::getVariableValuesAtLocation(
    gsl::span<const size_t> odbRowsAtLocation,
    gsl::span<int> valuesAtLocation) const {
  // Assumes that values have been prepopulated with missing values.
  if (columnIndex_ == -1)
    return;

  auto valueIt = valuesAtLocation.begin();
  auto rowIt = odbRowsAtLocation.begin();
  for (; valueIt != valuesAtLocation.end() && rowIt != odbRowsAtLocation.end(); ++rowIt) {
    const int value = static_cast<int>(sqlData_.getData(*rowIt, columnIndex_));
    if (value != odb_missing_int)
      *(valueIt++) = value;
  }
}

void VariableReaderFromRowsWithNonMissingValues::getVariableValuesAtLocation(
    gsl::span<const size_t> odbRowsAtLocation,
    gsl::span<float> valuesAtLocation) const {
  // Assumes that values have been prepopulated with missing values.
  if (columnIndex_ == -1)
    return;

  auto valueIt = valuesAtLocation.begin();
  auto rowIt = odbRowsAtLocation.begin();
  for (; valueIt != valuesAtLocation.end() && rowIt != odbRowsAtLocation.end(); ++rowIt) {
    const float value = static_cast<float>(sqlData_.getData(*rowIt, columnIndex_));
    if (value != odb_missing_float)
      *(valueIt++) = value;
  }
}

void VariableReaderFromRowsWithNonMissingValues::getVariableValuesAtLocation(
    gsl::span<const size_t> odbRowsAtLocation,
    gsl::span<std::string> valuesAtLocation) const {
  // Assumes that values have been prepopulated with missing values or empty strings.
  if (columnIndex_ == -1)
    return;

  auto valueIt = valuesAtLocation.begin();
  auto rowIt = odbRowsAtLocation.begin();
  for (; valueIt != valuesAtLocation.end() && rowIt != odbRowsAtLocation.end(); ++rowIt) {
    const double value = (sqlData_.getData(*rowIt, columnIndex_));
    if (value != odb_missing_float)
      *(valueIt++) = convertTo<std::string>(value);
  }
}

void VariableReaderFromRowsWithNonMissingValues::getVariableValuesAtLocation(
    gsl::span<const size_t> odbRowsAtLocation,
    gsl::span<char> valuesAtLocation) const {
  // Assumes that values have been prepopulated with zeros.
  if (columnIndex_ == -1)
    return;

  auto valueIt = valuesAtLocation.begin();
  auto rowIt = odbRowsAtLocation.begin();
  for (; valueIt != valuesAtLocation.end() && rowIt != odbRowsAtLocation.end(); ++rowIt) {
    const std::uint64_t value = static_cast<std::uint64_t>(sqlData_.getData(*rowIt, columnIndex_));
    if (value != odb_missing_int)
      *(valueIt++) = (value & bitfieldMask_) ? 1 : 0;
  }
}

// -----------------------------------------------------------------------------

VariableReaderFromRowsWithMatchingVarnos::VariableReaderFromRowsWithMatchingVarnos(
    const Parameters_ &parameters, const std::string &column, const std::string &member,
    const DataFromSQL &sqlData)
  : parameters_(parameters), member_(member), sqlData_(sqlData) {
  valueColumnIndex_ = sqlData.getColumnIndex(column);
  varnoColumnIndex_ = sqlData.getColumnIndex("varno");
  if (valueColumnIndex_ >= 0 && !member.empty())
    bitfieldMask_ = bitfieldMask(column, member, sqlData);
}

void VariableReaderFromRowsWithMatchingVarnos::getVariableValuesAtLocation(
    gsl::span<const size_t> odbRowsAtLocation,
    gsl::span<int> valuesAtLocation) const {
  getTypedVariableValuesAtLocation(odbRowsAtLocation, valuesAtLocation);
}

void VariableReaderFromRowsWithMatchingVarnos::getVariableValuesAtLocation(
    gsl::span<const size_t> odbRowsAtLocation,
    gsl::span<float> valuesAtLocation) const {
  getTypedVariableValuesAtLocation(odbRowsAtLocation, valuesAtLocation);
}

void VariableReaderFromRowsWithMatchingVarnos::getVariableValuesAtLocation(
    gsl::span<const size_t> odbRowsAtLocation,
    gsl::span<std::string> valuesAtLocation) const {
  getTypedVariableValuesAtLocation(odbRowsAtLocation, valuesAtLocation);
}

template <typename T>
void VariableReaderFromRowsWithMatchingVarnos::getTypedVariableValuesAtLocation(
    gsl::span<const size_t> odbRowsAtLocation,
    gsl::span<T> valuesAtLocation) const {
  // Assumes that values have been prepopulated with missing values.
  if (valueColumnIndex_ == -1 || varnoColumnIndex_ == -1)
    return;

  auto valueIt = valuesAtLocation.begin();
  for (int targetVarno : parameters_.varnos.value()) {
    auto rowIt = odbRowsAtLocation.begin();
    for (; valueIt != valuesAtLocation.end() && rowIt != odbRowsAtLocation.end(); ++rowIt) {
      const int varno = static_cast<int>(sqlData_.getData(*rowIt, varnoColumnIndex_));
      if (varno == targetVarno)
        *(valueIt++) = convertTo<T>(sqlData_.getData(*rowIt, valueColumnIndex_));
    }
  }
}

void VariableReaderFromRowsWithMatchingVarnos::getVariableValuesAtLocation(
    gsl::span<const size_t> odbRowsAtLocation,
    gsl::span<char> valuesAtLocation) const {
  // Assumes that values have been prepopulated with missing values.
  if (valueColumnIndex_ == -1 || varnoColumnIndex_ == -1)
    return;

  auto valueIt = valuesAtLocation.begin();
  for (int targetVarno : parameters_.varnos.value()) {
    auto rowIt = odbRowsAtLocation.begin();
    for (; valueIt != valuesAtLocation.end() && rowIt != odbRowsAtLocation.end(); ++rowIt) {
      const int varno = static_cast<int>(sqlData_.getData(*rowIt, varnoColumnIndex_));
      if (varno == targetVarno) {
        const std::uint64_t value = static_cast<std::uint64_t>(
              sqlData_.getData(*rowIt, valueColumnIndex_));
        *(valueIt++) = (value != odb_missing_int && value & bitfieldMask_) ? 1 : 0;
      }
    }
  }
}

}  // namespace ODC
}  // namespace Engines
}  // namespace ioda
