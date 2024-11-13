/*
 * (C) Copyright 2020-2021 UCAR
 * (C) Crown copyright 2021-2024, Met Office
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 */
/*! \addtogroup ioda_cxx_engines_pub_ODC
 *
 * @{
 * \file ODC.cpp
 * \brief ODB / ODC engine bindings
 */
#include <algorithm>
#include <set>
#include <string>
#include <typeinfo>
#include <vector>
#include <ctime>

#if USE_BOOST_REGEX
#include <boost/regex.hpp>
#define REGEX_NAMESPACE boost
#else
#include <regex>
#define REGEX_NAMESPACE std
#endif

#include "eckit/io/MemoryHandle.h"
#include "eckit/utils/StringTools.h"
#include "ioda/Engines/ODC.h"
#include "ioda/Exception.h"
#include "ioda/Group.h"
#include "ioda/ObsGroup.h"
#include "ioda/config.h"  // Auto-generated. Defines *_FOUND.
#include "ioda/Types/Type.h"
#include "oops/util/AssociativeContainers.h"
#include "oops/util/Logger.h"

#if odc_FOUND
# include "eckit/config/LocalConfiguration.h"
# include "eckit/config/YAMLConfiguration.h"
# include "eckit/filesystem/PathName.h"
# include "odc/api/odc.h"
# include "odc/Writer.h"
# include "ioda/Engines/ODC/DataFromSQL.h"
# include "ioda/Engines/ODC/OdbQueryParameters.h"
# include "ioda/Engines/ODC/ChannelIndexerFactory.h"
# include "ioda/Engines/ODC/ObsGroupTransformFactory.h"
# include "ioda/Engines/ODC/RowsIntoLocationsSplitterFactory.h"
# include "ioda/Engines/ODC/VariableCreator.h"
# include "ioda/Engines/ODC/VariableReaderFactory.h"
# include "../../Layouts/Layout_ObsGroup_ODB_Params.h"
#endif

namespace ioda {
namespace Engines {
namespace ODC {

namespace {

// -------------------------------------------------------------------------------------------------
// Initialization

#if odc_FOUND
/// @brief Function initializes the ODC API, just once.
void initODC() {
  static bool inited = false;
  if (!inited) {
    odc_initialise_api();
    inited = true;
  }
}
#else
/// @brief Standard message when the ODC API is unavailable.
const char odcMissingMessage[] {
  "The ODB / ODC engine is disabled because the odc library was "
  "not found at compile time."};
#endif


#if odc_FOUND

// -------------------------------------------------------------------------------------------------
// (Very simple) SQL expression parsing

/// Parsed SQL column expression.
struct ParsedColumnExpression {
  /// If `expression` is a bitfield column member name (of the form `column.member[@table]`,
  /// where `@table` is optional), split it into the column name `column[@table]` and member
  /// name `member`. Otherwise leave it unchanged.
  explicit ParsedColumnExpression(const std::string &expression) {
    static const REGEX_NAMESPACE::regex re("(\\w+)(?:\\.(\\w+))?(?:@(.+))?");
    REGEX_NAMESPACE::smatch match;
    if (REGEX_NAMESPACE::regex_match(expression, match, re)) {
      // This is an identifier of the form column[.member][@table]
      column += match[1].str();
      if (match[3].length()) {
         column += '@';
         column += match[3].str();
      }
      member = match[2];
    } else {
      // This is a more complex expression
      column = expression;
    }
  }

  std::string column;  //< Column name (possibly including table name) or a more general expression
  std::string member;  //< Bitfield member name (may be empty)
};

bool operator<(const ParsedColumnExpression & a, const ParsedColumnExpression &b) {
  return a.column < b.column || (a.column == b.column && a.member < b.member);
}

// -------------------------------------------------------------------------------------------------
// Query file parsing

/// \brief The set of ODB column members selected by the query file.
///
/// (Only bitfield columns have members; other columns can only be selected as a whole. Bitfield
/// columns can also be selected as a whole.)
class MemberSelection {
 public:
  /// Create an empty selection.
  MemberSelection() = default;

  /// Return true if the whole column has been selected, false otherwise.
  bool allMembersSelected() const { return allMembersSelected_; }
  /// Return the set of selected members or an empty set if the whole column has been selected.
  const std::set<std::string> &selectedMembers() const { return selectedMembers_; }

  /// Add member `member` to the selection.
  void addMember(const std::string &member) {
    if (!allMembersSelected_)
      selectedMembers_.insert(member);
  }

  /// Add all members to the selection.
  void addAllMembers() {
    allMembersSelected_ = true;
    selectedMembers_.clear();
  }

  /// Return the intersection of `members` with the set of selected members.
  std::set<std::string> intersectionWith(const std::set<std::string> &members) const {
    if (allMembersSelected()) {
      return members;
    } else {
      std::set<std::string> intersection;
      std::set_intersection(members.begin(), members.end(),
                            selectedMembers_.begin(), selectedMembers_.end(),
                            std::inserter(intersection, intersection.end()));
      return intersection;
    }
  }

 private:
  std::set<std::string> selectedMembers_;
  /// True if the column has been selected as a whole (i.e. effectively all members are selected)
  bool allMembersSelected_ = false;
};

/// The set of ODB columns selected by the query file (possibly only partially, i.e. including only
/// a subset of bitfield members).
class ColumnSelection {
 public:
  ColumnSelection() = default;

  void addColumn(const std::string &column) {
    members_[column].addAllMembers();
  }

  void addColumnMember(const std::string &column, const std::string &member) {
    members_[column].addMember(member);
  }

  /// Return the sorted list of
  std::vector<std::string> columns() const {
    std::vector<std::string> result;
    for (const auto &columnAndMembers : members_) {
      result.push_back(columnAndMembers.first);
    }
    return result;
  }

  const MemberSelection &columnMembers(const std::string &column) const {
    return members_.at(column);
  }

 private:
  std::map<std::string, MemberSelection> members_;
};

/// Select columns and column members specified in the `variables` list in the query file.
void addQueryColumns(ColumnSelection &selection, const OdbQueryParameters &queryParameters) {
  for (const OdbVariableParameters &varParameters : queryParameters.variables.value()) {
    ParsedColumnExpression parsedSource(varParameters.name);
    if (parsedSource.member.empty()) {
      selection.addColumn(parsedSource.column);
    } else {
      selection.addColumnMember(parsedSource.column, parsedSource.member);
    }
  }
}

// -------------------------------------------------------------------------------------------------
// Functions used by the ODB file reader.

/// \brief Creates dimension scales for the ObsGroup that will receive data loaded from an ODB file.
NewDimensionScales_t makeDimensionScales(const RowsByLocation &rowsByLocation,
                                         const ChannelIndexerBase *channelIndexer,
                                         const DataFromSQL &sqlData) {
  NewDimensionScales_t scales;

  const int numLocations = rowsByLocation.size();
  scales.push_back(
    NewDimensionScale<int>("Location", numLocations, numLocations, numLocations));

  if (channelIndexer) {
    const std::vector<int> channelIndices = channelIndexer->channelIndices(rowsByLocation, sqlData);
    const int numChannels = channelIndices.size();
    scales.push_back(
      NewDimensionScale<int>("Channel", numChannels, numChannels, numChannels));
  }

  return scales;
}

/// \brief Creates the Channel variable, the sole ioda variable without a Location dimension.
void createChannelVariable(ObsGroup &og, const ChannelIndexerBase &channelIndexer,
                           const RowsByLocation &rowsByLocation, const DataFromSQL &sqlData) {
  const std::vector<int> channelIndices = channelIndexer.channelIndices(rowsByLocation, sqlData);
  ioda::Variable v = og.vars["Channel"];
  v.write(channelIndices);
}

template <typename T>
bool contains(const std::vector<T> &vector, const T& element) {
  return std::find(vector.begin(), vector.end(), element) != vector.end();
}

template <typename T>
bool containsAny(const std::vector<T> &vector, const std::vector<T>& elements) {
  return std::any_of(elements.begin(), elements.end(),
                     [&vector] (const T &element) { return contains(vector, element); });
}

bool isSourceInQuery(const ParsedColumnExpression &source,
                     const std::set<ParsedColumnExpression> &queryContents) {
  if (source.member.empty())
    return oops::contains(queryContents, source);
  else
    return oops::contains(queryContents, source) ||
           oops::contains(queryContents, ParsedColumnExpression(source.column));
}

/// \brief Constructs and returns a vector of objects that will be used to create location-dependent
/// ioda variables holding data loaded from an ODB file.
std::vector<VariableCreator> makeVariableCreators(
    const detail::ODBLayoutParameters &layoutParams,
    const OdbQueryParameters &queryParams,
    const std::vector<int> &availableVarnos) {
  std::vector<VariableCreator> variableCreators;

  std::set<ParsedColumnExpression> queryContents;
  for (const auto &columns : queryParams.variables.value())
    queryContents.insert(ParsedColumnExpression(columns.name));

  const OdbVariableCreationParameters &varCreationParams = queryParams.variableCreation;

  // Handle varno-independent columns
  for (const detail::VariableParameters &columnParams : layoutParams.variables.value()) {
    // Skip columns meant to be written, but not read.
    if (columnParams.mode.value() == detail::IoMode::WRITE) {
      continue;
    }

    // Skip sources absent from the query.
    ParsedColumnExpression parsedSource(columnParams.source);
    if (!isSourceInQuery(parsedSource, queryContents))
      continue;

    const VariableReaderParametersBase *readerParams = nullptr;
    if (columnParams.reader.value() != boost::none)
      readerParams = &columnParams.reader.value()->params.value();
    else
      readerParams = &varCreationParams.defaultReader.value().params.value();

    VariableCreator creator(columnParams.name,
                            parsedSource.column,
                            parsedSource.member,
                            columnParams.multichannel,
                            *readerParams);
    variableCreators.push_back(std::move(creator));
  }

  const std::set<int> multichannelVarnos(varCreationParams.multichannelVarnos.value().begin(),
                                         varCreationParams.multichannelVarnos.value().end());
  // TODO(someone): Handle the case of the 'varno' option being set to ALL.
  const std::vector<int> &queriedVarnos =
      queryParams.where.value().varno.value().as<std::vector<int>>();

  // Handle varno-dependent columns
  for (const detail::VarnoDependentColumnParameters &columnParams :
       layoutParams.varnoDependentColumns.value()) {
    ParsedColumnExpression parsedSource(columnParams.source);
    for (const detail::VarnoToVariableNameMappingParameters &mappingParams :
         columnParams.mappings.value()) {
      // Skip sources absent from the query.
      if (!isSourceInQuery(parsedSource, queryContents))
        continue;

      // Skip varnos absent from the query.
      if (!contains(queriedVarnos, mappingParams.varno.value()) &&
          !containsAny(queriedVarnos, mappingParams.auxiliaryVarnos.value()))
        continue;

      if (varCreationParams.skipMissingVarnos.value()) {
        // Skip varnos absent from the input ODB file.
        if (!contains(availableVarnos, mappingParams.varno.value()) &&
            !containsAny(availableVarnos, mappingParams.auxiliaryVarnos.value()))
          continue;
      }

      std::string variableName;
      if (parsedSource.member.empty())
        variableName = parsedSource.column;
      else
        variableName = parsedSource.column + "." + parsedSource.member;
      variableName += "/";
      variableName += std::to_string(mappingParams.varno);

      std::vector<int> varnos{mappingParams.varno.value()};
      varnos.insert(varnos.end(), mappingParams.auxiliaryVarnos.value().begin(),
                    mappingParams.auxiliaryVarnos.value().end());

      detail::VariableReaderParameters readerParams;
      eckit::LocalConfiguration readerConfig;
      readerConfig.set("type", "from rows with matching varnos");
      readerConfig.set("varnos", varnos);
      readerParams.validateAndDeserialize(readerConfig);

      const bool hasChannelAxis = oops::contains(multichannelVarnos, mappingParams.varno);
      VariableCreator creator(variableName,
                              parsedSource.column,
                              parsedSource.member,
                              hasChannelAxis,
                              readerParams.params.value());
      variableCreators.push_back(std::move(creator));
    }
  }

  // Handle complementary variables
  for (const detail::ComplementaryVariablesParameters &complementaryVariablesParams :
       layoutParams.complementaryVariables.value()) {
    for (const std::string &columnName : complementaryVariablesParams.inputNames.value()) {
      // Skip columns absent from the query.
      if (!isSourceInQuery(ParsedColumnExpression(columnName), queryContents))
        continue;

      VariableCreator creator(columnName,
                              columnName,
                              "",
                              false /*has channel axis?*/,
                              varCreationParams.defaultReader.value().params.value());
      variableCreators.push_back(std::move(creator));
    }
  }

  /// \brief Constructs objects that will create temporary variables holding data loaded from ODB
  /// columns with dates and times. These will subsequently be transformed by
  /// CreateDateTimeTransform into variables storing datetimes in the ioda format, and the temporary
  /// variables (with names starting with a double underscore) will be deleted.
  {
    const VariableReaderParametersBase &readerParams =
        varCreationParams.defaultReader.value().params.value();

    if (isSourceInQuery(ParsedColumnExpression("date"), queryContents))
      variableCreators.push_back(VariableCreator("MetaData/__date",
                                                 "date",
                                                 "", // member
                                                 false /*has channel axis?*/,
                                                 readerParams));
    if (isSourceInQuery(ParsedColumnExpression("time"), queryContents))
      variableCreators.push_back(VariableCreator("MetaData/__time",
                                                 "time",
                                                 "", // member
                                                 false /*has channel axis?*/,
                                                 readerParams));
    if (isSourceInQuery(ParsedColumnExpression("receipt_date"), queryContents))
      variableCreators.push_back(VariableCreator("MetaData/__receipt_date",
                                                 "receipt_date",
                                                 "", // member
                                                 false /*has channel axis?*/,
                                                 readerParams));
    if (isSourceInQuery(ParsedColumnExpression("receipt_time"), queryContents))
      variableCreators.push_back(VariableCreator("MetaData/__receipt_time",
                                                 "receipt_time",
                                                 "", // member
                                                 false /*has channel axis?*/,
                                                 readerParams));
  }

  return variableCreators;
}

/// \brief Creates a vector of objects transforming pairs of variables storing dates and times in
/// the ODB format into single variables storing datetimes in the ioda format.
std::vector<std::unique_ptr<ObsGroupTransformBase>> makeDateTimeTransforms(
    const ODC_Parameters &odcParameters,
    const std::vector<OdbVariableParameters> &variableParameters,
    const OdbVariableCreationParameters& varCreationParameters) {
  std::vector<std::unique_ptr<ObsGroupTransformBase>> transforms;

  bool hasDate = false, hasTime = false, hasReceiptDate = false, hasReceiptTime = false;
  for (const OdbVariableParameters &varParams : variableParameters) {
    if (varParams.name.value() == "date")
      hasDate = true;
    else if (varParams.name.value() == "time")
      hasTime = true;
    else if (varParams.name.value() == "receipt_date")
      hasReceiptDate = true;
    else if (varParams.name.value() == "receipt_time")
      hasReceiptTime = true;
  }

  // MetaData/dateTime
  if (hasDate && hasTime) {
    eckit::LocalConfiguration config;
    config.set("name", "create dateTime");
    config.set("clamp to window start", true);
    if (!varCreationParameters.timeDisplacement.value().empty())
      config.set("displace by", varCreationParameters.timeDisplacement.value());
    ObsGroupTransformParameters transformParameters;
    transformParameters.validateAndDeserialize(config);
    transforms.push_back(ObsGroupTransformFactory::create(transformParameters.params,
                                                          odcParameters, varCreationParameters));
  }

  // MetaData/receiptdateTime
  if (hasReceiptDate && hasReceiptTime) {
    eckit::LocalConfiguration config;
    config.set("name", "create dateTime");
    config.set("input date", "MetaData/__receipt_date");
    config.set("input time", "MetaData/__receipt_time");
    config.set("output", "MetaData/receiptdateTime");
    // TODO(someone): does this variable not need to be displaced like dateTime?
    // It wasn't in the original code, but this may be unintentional.
    ObsGroupTransformParameters transformParameters;
    transformParameters.validateAndDeserialize(config);
    transforms.push_back(ObsGroupTransformFactory::create(transformParameters.params,
                                                          odcParameters, varCreationParameters));
  }

  // MetaData/initialDateTime
  const bool writeInitialDateTime = hasDate && hasTime &&
    odcParameters.timeWindowExtendedLowerBound != util::missingValue<util::DateTime>();
  if (writeInitialDateTime) {
    eckit::LocalConfiguration config;
    config.set("name", "create dateTime");
    config.set("output", "MetaData/initialDateTime");
    ObsGroupTransformParameters transformParameters;
    transformParameters.validateAndDeserialize(config);
    transforms.push_back(ObsGroupTransformFactory::create(transformParameters.params,
                                                          odcParameters, varCreationParameters));
  }

  return transforms;
}

/// \brief Creates and returns a vector of objects applying extra transforms to an ObsGroup read
/// from an ODB file.
std::vector<std::unique_ptr<ObsGroupTransformBase>> makeTransforms(
    const ODC_Parameters &odcParameters,
    const std::vector<OdbVariableParameters> &variableParameters,
    const OdbVariableCreationParameters& varCreationParameters) {
  // Date/time transforms are always applied as long as the required columns are in the query.
  std::vector<std::unique_ptr<ObsGroupTransformBase>> transforms = makeDateTimeTransforms(
        odcParameters, variableParameters, varCreationParameters);

  // The layout file may list extra transforms to be applied as well.
  for (const ObsGroupTransformParameters &transformParameters :
       varCreationParameters.transforms.value())
    transforms.push_back(ObsGroupTransformFactory::create(transformParameters.params,
                                                          odcParameters, varCreationParameters));

  return transforms;
}

// -------------------------------------------------------------------------------------------------
// Functions used by the ODB writer.

struct ReverseColumnMappings {
  std::map<std::string, std::string> varnoIndependentColumns;
  std::map<std::string, std::string> varnoDependentColumns;
  std::map<std::string, std::string> varnoDependentColumnsNames;
  std::map<std::string, std::string> complimentaryVariableColumns;
};

/// Parse the mapping file and return an object
/// * listing columns and bitfield column members for which a mapping to ioda variables has
///   been defined,
/// * indicating which of them should be treated as varno-dependent, and
/// * listing the varnos for which a mapping of each varno-dependent column or column member has
///   been defined.
ReverseColumnMappings collectReverseColumnMappings(const detail::ODBLayoutParameters &layoutParams,
                                                   const std::vector<std::string> &columns,
                                                   const std::vector<int> &listOfVarNos) {
  ReverseColumnMappings mappings;

  // Process varno-independent columns
  for (const detail::VariableParameters &columnParams : layoutParams.variables.value()) {      
    if (columnParams.mode.value() == detail::IoMode::READ) {
      // This column is meant to be read, but not written.
      continue;
    }

    const auto it = std::find(columns.begin(), columns.end(), columnParams.source.value());
    if (it != columns.end())
      mappings.varnoIndependentColumns[columnParams.name.value()] = columnParams.source.value();
  }

  // Add some default and an optional variables if not present
  if (mappings.varnoIndependentColumns.find("MetaData/latitude") == mappings.varnoIndependentColumns.end())
    mappings.varnoIndependentColumns["MetaData/latitude"] = "lat";
  if (mappings.varnoIndependentColumns.find("MetaData/longitude") == mappings.varnoIndependentColumns.end())
    mappings.varnoIndependentColumns["MetaData/longitude"] = "lon";
  if (mappings.varnoIndependentColumns.find("MetaData/dateTime") == mappings.varnoIndependentColumns.end())
    mappings.varnoIndependentColumns["MetaData/dateTime"] = "date";
  if (std::find(columns.begin(), columns.end(), "receipt_date") != columns.end() &&
      mappings.varnoIndependentColumns.find("MetaData/receiptdateTime") == mappings.varnoIndependentColumns.end())
    mappings.varnoIndependentColumns["MetaData/receiptdateTime"] = "receipt_date";

  for (const detail::VarnoDependentColumnParameters &columnParams :
       layoutParams.varnoDependentColumns.value()) {
    if (columnParams.source.value() == std::string("initial_obsvalue")) {
      for (const auto &mappingParams : columnParams.mappings.value()) {
        const auto it = std::find(listOfVarNos.begin(), listOfVarNos.end(), mappingParams.varno);
        if (it != listOfVarNos.end())
            mappings.varnoDependentColumns[mappingParams.name.value()] = std::to_string(mappingParams.varno);
      }
    }
  }
  // Create name mapping for varno dependent columns
  for (const detail::VarnoDependentColumnParameters &columnParams :
       layoutParams.varnoDependentColumns.value()) {
    const auto it = std::find(columns.begin(), columns.end(), columnParams.source.value());
    if (it != columns.end()) {
      for (const auto &map : columnParams.mappings.value()) {
        const auto varnoIt = std::find(listOfVarNos.begin(), listOfVarNos.end(), map.varno);
        if (varnoIt != listOfVarNos.end()) {
          const std::string ioda_variable_name = columnParams.groupName.value() +
                                                 "/" + map.name.value();
          mappings.varnoDependentColumnsNames[ioda_variable_name] = columnParams.source.value();
        }
      }
    }
  }
  return mappings;
}

struct ColumnInfo {
  std::string column_name;
  TypeClass column_type;
  int column_size;
  int string_length;
  int epoch_year;
  int epoch_month;
  int epoch_day;
  int epoch_hour;
  int epoch_minute;
  int epoch_second;
};

void pushBackVector(std::vector<std::vector<double>> &data_store,
                    const std::vector<double> &inarray,
                    const size_t numlocs, const size_t numchans) {
  if (numchans == 0) {
    ASSERT(inarray.size() == numlocs);
    data_store.push_back(inarray);
  } else if (inarray.size() == numlocs) {
    std::vector<double> data_store_tmp_chans(numlocs * numchans);
    // Copy each location value to all channels
    for (size_t j = 0; j < inarray.size(); j++)
      for (size_t i = 0; i < numchans; i++)
        data_store_tmp_chans[j * numchans + i] = inarray[j];
    data_store.push_back(data_store_tmp_chans);
  } else if (inarray.size() == numchans) {
    std::vector<double> data_store_tmp_chans(numlocs * numchans);
    // Copy each channel value to all channels
    for (size_t j = 0; j < numlocs; j++)
      for (size_t i = 0; i < numchans; i++)
        data_store_tmp_chans[j * numchans + i] = inarray[i];
    data_store.push_back(data_store_tmp_chans);
  } else if (inarray.size() == numchans * numlocs) {
    data_store.push_back(inarray);
  } else {
    oops::Log::info() << "inarray.size() = " << inarray.size() << std::endl;
    oops::Log::info() << "numlocs = " << numlocs << std::endl;
    oops::Log::info() << "numchans = " << numchans << std::endl;
    eckit::Abort("Attempt to write a vector that does not match a given size when writing "
                 "to the ODB file.  Array must be of size numlocs or numchans or numchans*numlocs");
  }
}

std::vector<int> getChannelNumbers(const Group &storageGroup) {
  TypeClass t = storageGroup.vars["Channel"].getType().getClass();
  std::vector<int> Channel;
  if (t == TypeClass::Integer) {
    storageGroup.vars["Channel"].read<int>(Channel);
  } else {
    std::vector<float> ChannelFloat;
    storageGroup.vars["Channel"].read<float>(ChannelFloat);
    for (size_t i = 0; i < ChannelFloat.size(); ++i)
      Channel.emplace_back(static_cast<int>(ChannelFloat[i]));
  }
  return Channel;
}

void setupColumnInfo(const Group &storageGroup, const std::map<std::string, std::string> &reverseColumnMap,
                     std::vector<ColumnInfo> &column_infos, int &num_columns,
                     const bool errorWithColumnNotInObsSpace, const bool ignoreChannels) {
  const auto objs = storageGroup.listObjects(ObjectType::Variable, true);
  for (auto it = objs.cbegin(); it != objs.cend(); it++) {
    for (size_t i = 0; i < it->second.size(); i++) {
      const auto found = reverseColumnMap.find(it->second[i]);
      if (found != reverseColumnMap.end()) {
        if (!it->second[i].compare(metadata_prefix_size,it->second[i].size(),"dateTime") ||
            !it->second[i].compare(metadata_prefix_size,it->second[i].size(),"receiptdateTime")) {
          std::string datename = "date";
          std::string timename = "time";
          const std::string obsspacename = it->second[i];
          if (obsspacename == "MetaData/receiptdateTime") {
            datename = "receipt_date";
            timename = "receipt_time";
          }
          ColumnInfo date, time;
          date.column_name = datename;
          date.column_type = storageGroup.vars[obsspacename].getType().getClass();
          date.column_size = storageGroup.vars[obsspacename].getType().getSize();
          std::string epochString = storageGroup.vars[obsspacename].atts.open("units").read<std::string>();
          std::size_t pos = epochString.find("seconds since ");
          epochString = epochString.substr(pos+14);
          const int year = std::stoi(epochString.substr(0,4).c_str());
          date.epoch_year = year;
          time.epoch_year = year;
          const int month = std::stoi(epochString.substr(5,2).c_str());
          date.epoch_month = month;
          time.epoch_month = month;
          const int day = std::stoi(epochString.substr(8,2).c_str());
          date.epoch_day = day;
          time.epoch_day = day;
          const int hour = std::stoi(epochString.substr(11,2).c_str());
          date.epoch_hour = hour;
          time.epoch_hour = hour;
          const int minute = std::stoi(epochString.substr(14,2).c_str());
          date.epoch_minute = minute;
          time.epoch_minute = minute;
          const int second = std::stoi(epochString.substr(17,2).c_str());
          date.epoch_second = second;
          time.epoch_second = second;
          date.string_length = 0;
          num_columns++;
          time.column_name = timename;
          time.column_type = storageGroup.vars[obsspacename].getType().getClass();
          time.column_size = storageGroup.vars[obsspacename].getType().getSize();
          time.string_length = 0;
          num_columns++;
          column_infos.push_back(date);
          column_infos.push_back(time);
        } else {
          ColumnInfo col;
          col.column_name = it->second[i];
          col.column_type = storageGroup.vars[col.column_name].getType().getClass();
          col.column_size = storageGroup.vars[col.column_name].getType().getSize();
          if (col.column_type == TypeClass::String) {
            std::vector<std::string> buf;
            storageGroup.vars[col.column_name].read<std::string>(buf);
            size_t len = 0;
            for (size_t j = 0; j < buf.size(); j++) {
              if (buf[j].size() > len) {
                len = buf[j].size();
              }
            }
            col.string_length = len;
            num_columns += 1+((col.string_length-1)/8);
          } else {
            col.string_length = 0;
            num_columns++;
          }
          column_infos.push_back(col);
        }
      }
      if (!it->second[i].compare("Channel") && !ignoreChannels) {
        ColumnInfo col;
        col.column_name = "vertco_reference_1";
        col.column_type = storageGroup.vars["Channel"].getType().getClass();
        col.column_size = storageGroup.vars["Channel"].getType().getSize();
        col.string_length = 0;
        num_columns++;
        column_infos.push_back(col);
      }
    }
  }
  // Check that map entry requested is in the ObsGroup
  for (const auto &map : reverseColumnMap) {
    if (!storageGroup.vars.exists(map.first)) {
      if (errorWithColumnNotInObsSpace) {
        throw eckit::UserError("Variable " + map.first +
                               " requested via the query file is not in the ObsSpace " +
                               "therefore aborting as requested", Here());
      } else {
        oops::Log::warning() << "WARNING: Variable " + map.first + " is in query file "
                             << "but not in ObsSpace therefore not being written out"
                             << std::endl;
      }
    } // end of if found
  } // end of map loop
  // Add the processed data column
  ColumnInfo col;
  col.column_name = "processed_data";
  col.column_type = TypeClass::Integer;
  col.column_size = 4;
  col.string_length = 0;
  num_columns++;
  column_infos.push_back(col);
}

void setupBodyColumnInfo(const Group &storageGroup, const std::map<std::string, std::string> &reverseColumnMap,
                     std::vector<ColumnInfo> &column_infos, std::vector<ColumnInfo> &column_infos_missing,
                     int &num_columns, const bool errorWithColumnNotInObsSpace) {
  std::vector<std::string> col_names;
  std::vector<std::string> obs_space_found;
  const auto objs = storageGroup.listObjects(ObjectType::Variable, true);
  for (auto it = objs.cbegin(); it != objs.cend(); it++) {
    for (size_t i = 0; i < it->second.size(); i++) {
      const auto found = reverseColumnMap.find(it->second[i]);
      if (found != reverseColumnMap.end()) {
        obs_space_found.push_back(it->second[i]);
        const auto alreadyExists = std::find(col_names.begin(), col_names.end(), found->second);
        if (alreadyExists != col_names.end()) continue;
        col_names.push_back(found->second);
        ColumnInfo col;
        col.column_name = found->second;
        const std::string obsspacename = it->second[i];
        col.column_type = storageGroup.vars[obsspacename].getType().getClass();
        col.column_size = storageGroup.vars[obsspacename].getType().getSize();
        if (col.column_type == TypeClass::String) {
          std::vector<std::string> buf;
          storageGroup.vars[obsspacename].read<std::string>(buf);
          size_t len = 0;
          for (size_t j = 0; j < buf.size(); j++) {
            if (buf[j].size() > len) {
              len = buf[j].size();
            }
          }
          col.string_length = len;
          num_columns += 1+((col.string_length-1)/8);
        } else {
          col.string_length = 0;
          num_columns++;
        }
        column_infos.push_back(col);
      }
    }
  }
  // Check that map entry requested is in the ObsGroup
  // if not create add to the missing which will get written
  // out with missing data
  for (const auto& map : reverseColumnMap) {
    const auto found = std::find(obs_space_found.begin(), obs_space_found.end(), map.first);
    if (found == obs_space_found.end()) {
      const auto alreadyListed = std::find(col_names.begin(), col_names.end(), map.second);
      // Check its a new column rather than a missing varno
      if (alreadyListed == col_names.end()) {
        ColumnInfo col;
        col.column_name = map.second;
        col.column_type = TypeClass::Float;
        col.column_size = 4;
        column_infos_missing.push_back(col);
        col_names.push_back(map.second);
      }
      if (errorWithColumnNotInObsSpace) {
        throw eckit::UserError("Variable " + map.first +
                               " requested via the query file is not in the ObsSpace " +
                               "therefore aborting as requested", Here());
      } else {
        oops::Log::warning() << "WARNING: Variable " + map.first + " is in query file "
                             << "but not in ObsSpace therefore assumming float and writing out with missing data"
                             << std::endl;
      }
    } // end of if found
  } // end of map loop
}

void setODBColumn(std::map<std::string, std::string> &columnMappings,
                  const ColumnInfo v, odc::Writer<>::iterator writer, int &column_number) {
  std::map<std::string,std::string>::iterator it2;
  std::string colname2 = "";
  for (it2 = columnMappings.begin(); it2 != columnMappings.end(); it2++) {
    if (it2->first == v.column_name) {
      colname2 = it2->second;
    }
  }
  if (colname2 == "") {
    colname2 = v.column_name;
    if (colname2.rfind(metadata_prefix, 0) == 0) {
      colname2.erase(0, metadata_prefix_size);
    }
  }
  // transform name to lower case
  std::transform(colname2.begin(), colname2.end(), colname2.begin(),
      [](unsigned char c){ return std::tolower(c); });
  if (v.column_type == TypeClass::Integer) {
    writer->setColumn(column_number, colname2, odc::api::INTEGER);
    column_number++;
  } else if (v.column_type == TypeClass::String) {
    if (v.string_length <= 8) {
      writer->setColumn(column_number, colname2, odc::api::STRING);
      column_number++;
    } else {
      for (int i = 0; i < 1+((v.string_length-1)/8); i++) {
        writer->setColumn(column_number, colname2 + std::string("_") + std::to_string(i + 1),
                          odc::api::STRING);
        column_number++;
      }
    }
  } else {
    writer->setColumn(column_number, colname2, odc::api::REAL);
    column_number++;
  }
}

void setODBBodyColumn(const ColumnInfo &v, odc::Writer<>::iterator writer, int &column_number) {
  // Column size 1 is a bool, this will be put in the odb as an integer
  if (v.column_type == TypeClass::Integer || v.column_size == 1) {
    writer->setColumn(column_number, v.column_name, odc::api::INTEGER);
    column_number++;
  } else if (v.column_type == TypeClass::String) {
    if (v.string_length <= 8) {
      writer->setColumn(column_number, v.column_name, odc::api::STRING);
      column_number++;
    } else {
      for (int i = 0; i < 1+((v.string_length-1)/8); i++) {
        writer->setColumn(column_number, v.column_name + std::string("_") + std::to_string(i + 1),
                          odc::api::STRING);
        column_number++;
      }
    }
  } else {
    writer->setColumn(column_number, v.column_name, odc::api::REAL);
    column_number++;
  }
}

void setupVarnos(const Group &storageGroup, const std::vector<int> &listOfVarNos,
                 const std::map<std::string, std::string> &mapping,
                 const bool errorWithColumnNotInObsSpace,
                 std::vector<int> &varnos,
                 std::vector<std::string> &varno_names) {
  for (const auto &map : mapping) {
    const std::string derived_obsvalue_name = std::string(derived_obsvalue_prefix) + map.first;
    const std::string obsvalue_name = std::string(obsvalue_prefix) + map.first;
    if (storageGroup.vars.exists(obsvalue_name) ||
        storageGroup.vars.exists(derived_obsvalue_name)) {
      varnos.push_back(std::stoi(map.second));
      varno_names.push_back(map.first);
    } else {
      if (errorWithColumnNotInObsSpace) {
        throw eckit::UserError("varno associated with " + map.first +
                               " requested via the query file is not in the ObsSpace " +
                               "therefore aborting as requested", Here());
      } else {
        oops::Log::warning() << "WARNING: varno associated with " + map.first + " is in query file "
                             << "but not in ObsSpace therefore not being written out"
                             << std::endl;
      }
    }
  }
}

void fillFloatArray(const Group & storageGroup, const std::string varname,
                    const int numrows, std::vector<double> & outdata, std::string odbType, std::vector<int> extendeds) {
  const bool derived_varname = varname.rfind("Derived", 0) == 0;
  const bool metadata_varname = varname.rfind("MetaData", 0) == 0;
  const bool derived_odb = odbType == "derived";
  if (storageGroup.vars.exists(varname)) {
    std::vector<float> buffer;
    storageGroup.vars[varname].read<float>(buffer);
    const ioda::Variable var = storageGroup.vars[varname];
    const float fillValue  = ioda::detail::getFillValue<float>(var.getFillValue());
    if (derived_odb) {
      if (metadata_varname) {
        for (int j = 0; j < numrows; j++) {
          if (fillValue == buffer[j]) {
            outdata[j] = odb_missing_float;
          } else {
            outdata[j] = buffer[j];
          }
        }
      } else {
        for (int j = 0; j < numrows; j++) {
          if ((derived_varname && extendeds[j] == 0) || (!derived_varname && extendeds[j] == 1) || fillValue == buffer[j]) {
            outdata[j] = odb_missing_float;
          } else {
            outdata[j] = buffer[j];
          }
        }
      }
    } else {
      for (int j = 0; j < numrows; j++) {
        if (fillValue == buffer[j]) {
          outdata[j] = odb_missing_float;
        } else {
          outdata[j] = buffer[j];
        }
      }
    }
  } else {
    for (int j = 0; j < numrows; j++)
      outdata[j] = odb_missing_float;
  }
}

void fillIntArray(const Group &storageGroup, const std::string varname,
                  const int numrows, const int columnsize,
                  std::vector<double> &outdata) {
  if (storageGroup.vars.exists(varname)) {
    if (columnsize == 4) {
      std::vector<int> buf;
      storageGroup.vars[varname].read<int>(buf);
      const int fillValue = ioda::detail::getFillValue<int>(storageGroup.vars[varname].getFillValue());
      for (int j = 0; j < numrows; j++) {
        if (fillValue == buf[j]) {
          outdata[j] = odb_missing_int;
        } else {
          outdata[j] = buf[j];
        }
      }
    } else if (columnsize == 8) {
      std::vector<long> buf;
      long fillValue;
      Variable var = storageGroup.vars.open(varname);
      if (var.isA<long>()) {
        var.read<long>(buf);
        fillValue = ioda::detail::getFillValue<long>(var.getFillValue());
      } else if (var.isA<int64_t>()) {
        std::vector<int64_t> buf64;
        var.read<int64_t>(buf64);
        buf.reserve(buf64.size());
        buf.assign(buf64.begin(), buf64.end());
        fillValue = ioda::detail::getFillValue<int64_t>(var.getFillValue());
      } else {
        std::string errMsg("ODB Writer: Unrecognized data type for column size of 8");
        throw Exception(errMsg.c_str(), ioda_Here());
      }
      for (int j = 0; j < numrows; j++) {
        if (fillValue == buf[j]) {
          outdata[j] = odb_missing_int;
        } else {
          outdata[j] = buf[j];
        }
      }
    }
  } else {
    for (int j = 0; j < numrows; j++) outdata[j] = odb_missing_int;
  }
}

void readColumn(const Group &storageGroup, const ColumnInfo column, std::vector<std::vector<double>> &data_store,
                const int number_of_locations, const int number_of_channels, std::string odb_type, std::vector<int> extendeds) {
  if (column.column_name == "date" || column.column_name == "receipt_date") {
    std::string obsspacename = "MetaData/dateTime";
    if (column.column_name == "receipt_date") obsspacename = "MetaData/receiptdateTime";
    const int arraySize = storageGroup.vars[obsspacename].getDimensions().numElements;
    std::vector<double> data_store_date(arraySize);
    std::vector<int64_t> buf;    
    storageGroup.vars[obsspacename].read<int64_t>(buf);
    float fillValue = ioda::detail::getFillValue<float>(storageGroup.vars[obsspacename].getFillValue());
    for (int j = 0; j < arraySize; j++) {
      if (fillValue == buf[j]) {
        data_store_date[j] = odb_missing_float;
      } else {
        // struct tm is being used purely for time arithmetic.  The offset is incorrect but it
        // doesn't matter in this context.
        //
        // Note the struct tm type wants the year to be the number of years since 1900
        // and the month to be the number of months from January (ie, Jan to Dec -> 0 to 11)
        //
        // In order to avoid the 2038 issue (max positive value in a signed 32-bit int
        // offset in seconds referenced to Jan 1, 1970 represents a datetime in Jan 2038)
        // convert the offset in seconds to a set of offsets for seconds, minutes,
        // hours and days and add those to the repective data members of the struct tm.
        int64_t offset = buf[j];
        struct tm time = { 0 };
        time.tm_sec = column.epoch_second + (offset % 60);
        offset /= 60;
        time.tm_min = column.epoch_minute + (offset % 60);
        offset /= 60;
        time.tm_hour = column.epoch_hour + (offset % 24);
        offset /= 24;
        time.tm_mday = column.epoch_day + offset;
        time.tm_mon = column.epoch_month - 1;
        time.tm_year = column.epoch_year - 1900;
        timegm(&time);
        data_store_date[j] =
            (time.tm_year + 1900) * 10000 + (time.tm_mon + 1) * 100 + time.tm_mday;
      }
    }
    pushBackVector(data_store, data_store_date, number_of_locations, number_of_channels);
  } else if (column.column_name == "time" || column.column_name == "receipt_time") {
    std::string obsspacename = "MetaData/dateTime";
    if (column.column_name == "receipt_date") obsspacename = "MetaData/receiptdateTime";
    const int arraySize = storageGroup.vars[obsspacename].getDimensions().numElements;
    std::vector<double> data_store_time(arraySize);
    std::vector<int64_t> buf;    
    storageGroup.vars[obsspacename].read<int64_t>(buf);
    const float fillValue = ioda::detail::getFillValue<float>(storageGroup.vars[obsspacename].getFillValue());
    for (int j = 0; j < arraySize; j++) {
      if (fillValue == buf[j]) {
        data_store_time[j] = odb_missing_float;
      } else {
        // See comments above in the date section.
        int64_t offset = buf[j];
        struct tm time = { 0 };
        time.tm_sec = column.epoch_second + (offset % 60);
        offset /= 60;
        time.tm_min = column.epoch_minute + (offset % 60);
        offset /= 60;
        time.tm_hour = column.epoch_hour + (offset % 24);
        offset /= 24;
        time.tm_mday = column.epoch_day + offset;
        time.tm_mon = column.epoch_month - 1;
        time.tm_year = column.epoch_year - 1900;
        timegm(&time);
        data_store_time[j] = time.tm_hour * 10000 + time.tm_min * 100 + time.tm_sec;
      }
    }
    pushBackVector(data_store, data_store_time, number_of_locations, number_of_channels);
  } else if (column.column_name == "vertco_reference_1") {
    std::vector<int> buf = getChannelNumbers(storageGroup);
    std::vector<double> data_store_chan(number_of_locations * number_of_channels);
    for (int j = 0; j < number_of_locations; j++)
      for (int i = 0; i < number_of_channels; i++)
        data_store_chan[j * number_of_channels + i] = static_cast<double>(buf[i]);
    data_store.push_back(data_store_chan);
  } else if (column.column_name == "processed_data") {
    if (number_of_channels > 0) {
      std::vector<double> data_store_chan(number_of_locations * number_of_channels);
      for (int j = 0; j < number_of_locations; j++)
        for (int i = 0; i < number_of_channels; i++)
          data_store_chan[j * number_of_channels + i] = extendeds[j * number_of_channels + i];
      pushBackVector(data_store, data_store_chan, number_of_locations, number_of_channels);
    } else {
      std::vector<double> data_store_chan(number_of_locations);
      for (int j = 0; j < number_of_locations; j++)
          data_store_chan[j] = extendeds[j];
      pushBackVector(data_store, data_store_chan, number_of_locations, number_of_channels);
    }
  } else if (column.column_type == TypeClass::Float) {
    const int arraySize = storageGroup.vars[column.column_name].getDimensions().numElements;
    std::vector<double> data_store_float(arraySize);
    fillFloatArray(storageGroup, column.column_name,
                   arraySize, data_store_float, odb_type, extendeds);
    pushBackVector(data_store, data_store_float, number_of_locations, number_of_channels);
  } else if (column.column_type == TypeClass::Integer) {
    const int arraySize = storageGroup.vars[column.column_name].getDimensions().numElements;
    std::vector<double> data_store_int(arraySize);
    fillIntArray(storageGroup, column.column_name,
                 arraySize, column.column_size,
                 data_store_int);
    pushBackVector(data_store, data_store_int, number_of_locations, number_of_channels);
  } else if (column.column_type == TypeClass::String) {
    const int arraySize = storageGroup.vars[column.column_name].getDimensions().numElements;
    std::vector<double> data_store_string(arraySize);
    std::vector<std::string> buf;
    storageGroup.vars[column.column_name].read<std::string>(buf);
    int num_cols = 1+((column.string_length-1)/8);
    for (int c = 0; c < num_cols; c++) {
      for (int j = 0; j < arraySize; j++) {
         unsigned char uc[8];
         double dat;
         for (int k = 8 * c; k < std::min(8 * (c+1),static_cast<int>(buf[j].size())); k++) {
           uc[k-c*8] = buf[j][k];
         }
         for (int k = std::min(8 * (c+1),static_cast<int>(buf[j].size())); k < 8*(c+1); k++) {
           uc[k-c*8] = '\0';
         }
         memcpy(&dat, uc, 8);
         data_store_string[j] = dat;
      }
      pushBackVector(data_store, data_store_string, number_of_locations, number_of_channels);
    }
  } else if (column.column_type == TypeClass::Unknown) {
    const int arraySize = storageGroup.vars[column.column_name].getDimensions().numElements;
    std::vector<double> data_store_unknown(arraySize);
    for (int j = 0; j < arraySize; j++) {
      data_store_unknown[j] = -1.0;
    }
    pushBackVector(data_store, data_store_unknown, number_of_locations, number_of_channels);
  }
}

void readBodyColumns(const Group &storageGroup, const ColumnInfo &column, const std::string v,
                     const int number_of_rows,
                     const std::map<std::string, std::string> &reverseMap,
                     std::vector<std::vector<double>> &data_store, std::string odb_type, std::vector<int> extendeds) {
  // Work out the correct ObsSpace variable to read
  std::string obsspacename;
  for (auto const& x : reverseMap) {
    auto lastslash = x.first.find_last_of("/");
    std::string obsspacevar = x.first.substr(lastslash+1);
    if (obsspacevar == v && x.second == column.column_name)
        obsspacename = x.first;
  }
  // Create data_store_tmp
  std::vector<double> data_store_tmp(number_of_rows);
  if (column.column_type == TypeClass::Integer) {
      fillIntArray(storageGroup, obsspacename,
                   number_of_rows, column.column_size,
                   data_store_tmp);
  } else if (obsspacename.substr(0, obsspacename.find("/")) == "DiagnosticFlags") {
    std::vector<char> buf_char;
    storageGroup.vars[obsspacename].read<char>(buf_char);
    const ioda::Variable var = storageGroup.vars[obsspacename];
    const char fillValue = ioda::detail::getFillValue<char>(var.getFillValue());
    for (int j = 0; j < number_of_rows; j++) {
      if (fillValue == buf_char[j]) {
        data_store_tmp[j] = 0;
      } else {
        const int flag = buf_char[j] > 0;
        data_store_tmp[j] = flag;
      }
    }
  } else if (column.column_type == TypeClass::Float) {
    fillFloatArray(storageGroup, obsspacename,
                   number_of_rows, data_store_tmp, odb_type, extendeds);
  } else if (column.column_type == TypeClass::Integer) {
    fillIntArray(storageGroup, obsspacename,
                 number_of_rows, column.column_size,
                 data_store_tmp);
  } else if (column.column_type == TypeClass::String) {
    std::vector<std::string> buf;
    storageGroup.vars[obsspacename].read<std::string>(buf);
    int num_cols = 1+((column.string_length-1)/8);
    for (int c = 0; c < num_cols; c++) {
      for (int j = 0; j < number_of_rows; j++) {
         unsigned char uc[8];
         double dat;
         for (int k = 8 * c; k < std::min(8 * (c+1),static_cast<int>(buf[j].size())); k++) {
           uc[k-c*8] = buf[j][k];
         }
         for (int k = std::min(8 * (c+1),static_cast<int>(buf[j].size())); k < 8*(c+1); k++) {
           uc[k-c*8] = '\0';
         }
         memcpy(&dat, uc, 8);
         data_store_tmp[j] = dat;
      }
    }
  } else if (column.column_type == TypeClass::Unknown) {
    std::vector<double> data_store_tmp(number_of_rows);
    for (int j = 0; j < number_of_rows; j++) {
      data_store_tmp[j] = -1.0;
    }
  }
  // Push back tmp vector to input vector for output
  data_store.push_back(data_store_tmp);
}

void writeODB(const size_t num_varnos, const int number_of_rows, odc::Writer<>::iterator writer,
              const std::vector<std::vector<double>> &data_store,
              const std::vector<std::vector<std::vector<double>>> &data_body_store,
              const int num_indep, const int num_body,
              const int num_body_missing, const std::vector<int> &varnos) {
  for (int row = 0; row < number_of_rows; row++) {
    for (size_t varno = 0; varno < num_varnos; varno++) {
      int col_num = 0;
      // Varno independent variables
      for (int column = 0; column < num_indep; column++) {
        (*writer)[col_num] = data_store[column][row];
        col_num++;        
      }
      // Varno dependent variables
      if (num_varnos > 0) {
        (*writer)[col_num] = varnos[varno];
        col_num++;
        for (int column = 0; column < num_body; column++) {
          (*writer)[col_num] = data_body_store[column][varno][row];
          col_num++;
        }
      }
      // Missing varno dependent variables
      for (int column = 0; column < num_body_missing; column++) {
        (*writer)[col_num] = odb_missing_float;
        col_num++;
      }
      ++writer;
    }
  }
}

// -------------------------------------------------------------------------------------------------

#endif  // odc_FOUND

}  // namespace

Group createFile(const ODC_Parameters& odcparams, Group storageGroup) {

#if odc_FOUND
  const int number_of_locations = storageGroup.vars["Location"].getDimensions().dimsCur[0];
  std::vector<int> extendeds;
  int number_of_rows = number_of_locations;
  int number_of_channels = 0;
  if (storageGroup.vars.exists("Channel") &&  !odcparams.ignoreChannelDimensionWrite) {
     std::vector<int> channels = getChannelNumbers(storageGroup);
     number_of_rows *= channels.size();
     number_of_channels = channels.size();
  }
  if (storageGroup.vars.exists("MetaData/extendedObsSpace")) {
    storageGroup.vars["MetaData/extendedObsSpace"].read<int>(extendeds);
  } else {
    extendeds.resize(number_of_rows, 0);
  }

  // Read in the query file
  eckit::YAMLConfiguration conf(eckit::PathName(odcparams.queryFile));
  OdbQueryParameters queryParameters;
  queryParameters.validateAndDeserialize(conf);
  ColumnSelection columnSelection;
  addQueryColumns(columnSelection, queryParameters);
  const std::vector<int> &listOfVarNos =
          queryParameters.where.value().varno.value().as<std::vector<int>>();

  // Create mapping from ObsSpace to odb name
  detail::ODBLayoutParameters layoutParams;
  layoutParams.validateAndDeserialize(
        eckit::YAMLConfiguration(eckit::PathName(odcparams.mappingFile)));
  ReverseColumnMappings columnMappings = collectReverseColumnMappings(layoutParams,
                                                                      columnSelection.columns(),
                                                                      listOfVarNos);

  // Setup the varno independent columns and vectors
  int num_varnoIndependnet_columns = 0;
  std::vector<ColumnInfo> column_infos;
  setupColumnInfo(storageGroup, columnMappings.varnoIndependentColumns,
                  column_infos, num_varnoIndependnet_columns, odcparams.missingObsSpaceVariableAbort,
                  odcparams.ignoreChannelDimensionWrite);
  if (num_varnoIndependnet_columns == 0) return storageGroup;

  // Fill data_store with varno independent data
  // access to this store is [col][rows]
  std::vector<std::vector<double>> data_store;
  for (const auto& v: column_infos) {
    readColumn(storageGroup, v, data_store, number_of_locations, number_of_channels, odcparams.odbType, extendeds);
  }

  // Setup the varno dependent columns and vectors
  std::vector<int> varnos;
  std::vector<std::string> varno_names;
  setupVarnos(storageGroup, listOfVarNos,
              columnMappings.varnoDependentColumns,
              odcparams.missingObsSpaceVariableAbort,
              varnos, varno_names);
  std::vector<ColumnInfo> body_column_infos;
  std::vector<ColumnInfo> body_column_missing_infos;
  int num_body_columns = 0;
  setupBodyColumnInfo(storageGroup, columnMappings.varnoDependentColumnsNames,
                      body_column_infos, body_column_missing_infos, num_body_columns,
                      odcparams.missingObsSpaceVariableAbort);

  const size_t num_varnos = varnos.size();
  const int num_body_columns_missing = body_column_missing_infos.size();
  // 1 added on for varno col
  int total_num_cols = num_varnoIndependnet_columns + num_body_columns +
                       num_body_columns_missing + 1;

  // Read body columns in data store for body columns
  // access to this store is [col][varno][rows]
  std::vector<std::vector<std::vector<double>>> data_store_body;
  for (const auto& col: body_column_infos) {
    std::vector<std::vector<double>> data_tmp;
    for (const auto &varno: varno_names) {
      readBodyColumns(storageGroup, col, varno, number_of_rows,
                      columnMappings.varnoDependentColumnsNames, data_tmp, odcparams.odbType, extendeds);
    }
    data_store_body.push_back(data_tmp);
  }

  // Setup the odb writer object
  eckit::PathName p(odcparams.outputFile);
  odc::Writer<> oda(p);
  odc::Writer<>::iterator writer = oda.begin();

  // Setup to column information
  writer->setNumberOfColumns(total_num_cols);
  int column_number = 0;
  // Varno independent
  for (const auto& v: column_infos) {
    setODBColumn(columnMappings.varnoIndependentColumns, v, writer, column_number);
  }
  // Varno dependent
  writer->setColumn(column_number, "varno",  odc::api::INTEGER);
  column_number++;
  for (const auto& col: body_column_infos) {
    setODBBodyColumn(col, writer, column_number);
  }
  // Varno dependent not in the ObsSpace
  for (const auto& col: body_column_missing_infos) {
    setODBBodyColumn(col, writer, column_number);
  }
  // Write header and data to the ODB file
  writer->writeHeader();
  writeODB(num_varnos, number_of_rows, writer, data_store, data_store_body,
           num_varnoIndependnet_columns, num_body_columns,
           num_body_columns_missing, varnos);
#endif
  return storageGroup;
}

ObsGroup openFile(const ODC_Parameters& odcparams,
  Group storageGroup)
{
  // 1. Check first that the ODB engine is enabled. If the engine
  // is not enabled, then throw an exception.
#if odc_FOUND
  initODC();

  using std::set;
  using std::string;
  using std::vector;

  oops::Log::debug() << "ODC called with " << odcparams.queryFile << "  " <<
                        odcparams.mappingFile << std::endl;

  // 2. Extract the lists of columns and varnos to select from the query file.

  eckit::YAMLConfiguration conf(eckit::PathName(odcparams.queryFile));
  OdbQueryParameters queryParameters;
  queryParameters.validateAndDeserialize(conf);

  ColumnSelection columnSelection;
  addQueryColumns(columnSelection, queryParameters);

  // TODO(someone): Handle the case of the 'varno' option being set to ALL.
  const vector<int> &varnos = queryParameters.where.value().varno.value().as<std::vector<int>>();

  // 3. Perform the SQL query.

  DataFromSQL sql_data;
  {
    std::vector<std::string> columnNames = columnSelection.columns();

    // Temporary: Ensure that obsvalue, if present, is the last item. This ensures ODB
    // conversion tests produce output files with variables arranged in the same order as a previous
    // version of this code. The h5diff tool used by these tests is oddly sensitive to variable order.
    // In case of a future major change necessitating regeneration of the reference output files
    // this code can be removed.
    {
      const auto it = std::find(columnNames.begin(), columnNames.end(), "initial_obsvalue");
      if (it != columnNames.end()) {
        // Move the initial_obsvalue column to the end
        std::rotate(it, it + 1, columnNames.end());
      }
    }
    sql_data.select(columnNames,
                    odcparams.filename,
                    varnos,
                    queryParameters.where.value().query);
  }

  const std::unique_ptr<RowsIntoLocationsSplitterBase> rowsIntoLocationsSplitter =
      RowsIntoLocationsSplitterFactory::create(
        queryParameters.variableCreation.rowsIntoLocationsSplit.value().params);
  const RowsByLocation rowsByLocation = rowsIntoLocationsSplitter->groupRowsByLocation(sql_data);

  if (rowsByLocation.empty())
    return storageGroup;

  // 4. Create an ObsGroup, using the mapping file to set up the translation of ODB column names
  // to ioda variable names

  std::vector<std::string> ignores;
  ignores.push_back("Location");
  ignores.push_back("MetaData/__date");
  ignores.push_back("MetaData/__time");
  ignores.push_back("MetaData/__receipt_date");
  ignores.push_back("MetaData/__receipt_time");
  ignores.push_back("MetaData/dateTime");
  ignores.push_back("MetaData/receiptdateTime");
  // Write out MetaData/initialDateTime if 'time window extended lower bound' is non-missing.
  const util::DateTime missingDate = util::missingValue<util::DateTime>();
  const bool writeInitialDateTime =
    odcparams.timeWindowExtendedLowerBound != missingDate;
  if (writeInitialDateTime)
    ignores.push_back("MetaData/initialDateTime");
  ignores.push_back("Channel");

  std::unique_ptr<ChannelIndexerBase> channelIndexer;
  if (queryParameters.variableCreation.channelIndexing.value()) {
    channelIndexer = ChannelIndexerFactory::create(
          queryParameters.variableCreation.channelIndexing.value()->params);
  }

  NewDimensionScales_t dimensionScales = makeDimensionScales(
    rowsByLocation, channelIndexer.get(), sql_data);

  auto og = ObsGroup::generate(
    storageGroup,
    dimensionScales,
    detail::DataLayoutPolicy::generate(
      detail::DataLayoutPolicy::Policies::ObsGroupODB, odcparams.mappingFile, ignores));

  // 5. Determine which columns and bitfield column members are varno-dependent and which aren't
  detail::ODBLayoutParameters layoutParameters;
  layoutParameters.validateAndDeserialize(
        eckit::YAMLConfiguration(eckit::PathName(odcparams.mappingFile)));

  const std::vector<VariableCreator> variableCreators = makeVariableCreators(
    layoutParameters, queryParameters, sql_data.getVarnos());

  // 6. Populate the ObsGroup with variables

  ioda::VariableCreationParameters params;

  // 6.1. Create location-independent variables

  if (channelIndexer)
    createChannelVariable(og, *channelIndexer, rowsByLocation, sql_data);

  // 6.2. Create location-dependent variables

  for (const VariableCreator &creator : variableCreators) {
    creator.createVariable(og, params, rowsByLocation, sql_data);
  }

  std::vector<std::unique_ptr<ObsGroupTransformBase>> transforms = makeTransforms(
        odcparams, queryParameters.variables, queryParameters.variableCreation);
  for (const std::unique_ptr<ObsGroupTransformBase> &transform : transforms)
    transform->transform(og);

  og.vars.stitchComplementaryVariables();

  // Remove temporary variables, whose names start with a double underscore.
  // (It would be clearer to place them in a separate group, but ObsGroup doesn't provide a method
  // for removing a group.
  if (og.exists("MetaData")) {
    Group tempOdbId = og.open("MetaData");
    const std::vector<std::string> names = tempOdbId.vars.list();
    for (const std::string &name : names) {
      if (eckit::StringTools::startsWith(name, "__")) {
        og.vars.remove("MetaData/" + name);
      }
    }
  }

  return og;
#else
  throw Exception(odcMissingMessage, ioda_Here());
#endif
}

}  // namespace ODC
}  // namespace Engines
}  // namespace ioda
