/*
 * (C) Crown copyright 2024, Met Office
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 */

#include "ioda/Engines/ODC/ObsGroupTransform.h"

#include "ioda/Engines/ODC/OdbConstants.h"
#include "ioda/Engines/ODC/ObsGroupTransformFactory.h"
#include "ioda/ObsGroup.h"

#include "oops/util/DateTime.h"
#include "oops/util/missingValues.h"

#include <sstream>

namespace ioda {
namespace Engines {
namespace ODC {

namespace {
// Register transforms
ObsGroupTransformMaker<CreateDateTimeTransform> createDateTimeMaker("create dateTime");
ObsGroupTransformMaker<CreateStationIdTransform> createStationIdMaker(
    "create stationIdentification");
ObsGroupTransformMaker<ConcatenateVariablesTransform> concatenateVariablesMaker(
    "concatenate variables");

// -----------------------------------------------------------------------------

/// Convert an epoch string to a util::DateTime
// todo: keep unified with the version in IodaUtils.cc
util::DateTime getEpochAsDtime(std::string epochString) {
  // Strip off the "seconds since " part. For now,
  // we are restricting the units to "seconds since " and will be expanding that
  // in the future to other time units (hours, days, minutes, etc).
  std::size_t pos = epochString.find("seconds since ");
  if (pos == std::string::npos) {
    std::string errorMsg =
        std::string("For now, only supporting 'seconds since' form of ") +
        std::string("units for MetaData/dateTime variable");
    Exception(errorMsg.c_str(), ioda_Here());
  }
  epochString.replace(pos, pos+14, "");

  return util::DateTime(epochString);
}

}  // namespace

// -----------------------------------------------------------------------------

CreateDateTimeTransform::CreateDateTimeTransform(
    const Parameters_ &transformParameters, const ODC_Parameters &odcParameters,
    const OdbVariableCreationParameters &variableCreationParameters)
  : transformParameters_(transformParameters),
    odcParameters_(odcParameters), variableCreationParameters_(variableCreationParameters)
{}

void CreateDateTimeTransform::transform(ObsGroup &og) const
{
  const util::DateTime missingDate = util::missingValue<util::DateTime>();
  const util::DateTime &timeWindowStart = odcParameters_.timeWindowStart;
  const util::DateTime &timeWindowExtendedLowerBound =
      odcParameters_.timeWindowExtendedLowerBound;
  const bool useTimeWindowExtendedLowerBound = transformParameters_.clampToWindowStart &&
      timeWindowExtendedLowerBound != missingDate && timeWindowStart != missingDate;
  if (useTimeWindowExtendedLowerBound && timeWindowExtendedLowerBound > timeWindowStart) {
    throw eckit::UserError("'time window extended lower bound' must be less than or equal to "
                           "the start of the DA window.", Here());
  }

  const util::DateTime epoch = getEpochAsDtime(variableCreationParameters_.epoch);

  const std::vector<int> date = og.vars[transformParameters_.inputDate].readAsVector<int>();
  const std::vector<int> time = og.vars[transformParameters_.inputTime].readAsVector<int>();

  std::vector<int> timeDisp;
  if (transformParameters_.displaceBy.value() != boost::none) {
    timeDisp = og.vars[*transformParameters_.displaceBy.value()].readAsVector<int>();
  } else {
    timeDisp.resize(date.size(), odb_missing_int);
  }
  std::vector<int64_t> offsets;
  offsets.reserve(date.size());
  for (size_t i = 0; i < date.size(); ++i) {
    if (date[i] != odb_missing_int && time[i] != odb_missing_int) {
      const int year   = date[i] / 10000;
      const int month  = date[i] / 100 - year * 100;
      const int day    = date[i] - 10000 * year - 100 * month;
      const int hour   = time[i] / 10000;
      const int minute = time[i] / 100 - hour * 100;
      const int second = time[i] - 10000 * hour - 100 * minute;
      util::DateTime datetime(year, month, day, hour, minute, second);
      if (timeDisp[i] != odb_missing_int) {
        const util::Duration displacement(timeDisp[i]);
        datetime += displacement;
      }
      // If an extended lower bound on the time window has been set,
      // and this observation's datetime lies between that bound and the start of the
      // time window, move the datetime to the start of the time window.
      // This ensures that the observation will be accepted by the time
      // window cutoff that is applied in oops.
      // The original value of the datetime is stored in MetaData/initialDateTime.
      if (useTimeWindowExtendedLowerBound &&
          datetime > timeWindowExtendedLowerBound &&
          datetime <= timeWindowStart)
        datetime = timeWindowStart;
      const int64_t offset = (datetime - epoch).toSeconds();
      offsets.push_back(offset);
    } else {
      offsets.push_back(variableCreationParameters_.missingInt64);
    }
  }

  ioda::VariableCreationParameters varCreationParameters;
  varCreationParameters.setFillValue<int64_t>(variableCreationParameters_.missingInt64);
  ioda::Variable v = og.vars.createWithScales<int64_t>(
    transformParameters_.output, {og.vars["Location"]}, varCreationParameters);
  v.atts.add<std::string>("units", variableCreationParameters_.epoch);
  v.write(offsets);
}

// -----------------------------------------------------------------------------

void StationIdSourceParameters::deserialize(util::CompositePath &path,
                                            const eckit::Configuration &config) {
  Parameters::deserialize(path, config);
  if ((variable.value() == boost::none) == (wmoId.value() == boost::none))
    throw eckit::UserError(path.path() +
                           ": either `variable` or `wmo id` must be set, but not both");
}

CreateStationIdTransform::CreateStationIdTransform(const Parameters_ &parameters,
                                                   const ODC_Parameters &,
                                                   const OdbVariableCreationParameters &)
  : parameters_(parameters)
{}

void CreateStationIdTransform::transform(ObsGroup &og) const
{
  std::vector<std::string> stationIDs =
      og.vars[parameters_.destination.value()].readAsVector<std::string>();
  const size_t nlocs = stationIDs.size();
  std::vector<bool> alreadySet(nlocs, false);

  for (const StationIdSourceParameters &sourceParameters : parameters_.sources.value()) {
    if (sourceParameters.variable.value() != boost::none) {
      const VariableSourceParameters &variableSourceParameters =
          *sourceParameters.variable.value();
      const std::string &name = variableSourceParameters.name.value();
      if (!og.vars.exists(name))
        continue;

      const Variable source = og.vars[name];
      const TypeClass typeClass = source.getType().getClass();
      if (typeClass == TypeClass::Integer) {
        const std::vector<int> values = source.readAsVector<int>();
        std::ostringstream stream;
        for (size_t loc = 0; loc < nlocs; loc++) {
          if (!alreadySet[loc] && values[loc] != odb_missing_int) {
            stream.str("");
            if (variableSourceParameters.width.value() != boost::none)
              stream << std::setw(*variableSourceParameters.width.value());
            if (variableSourceParameters.padWithZeros.value())
              stream << std::setfill('0');
            stream << values[loc];
            stationIDs[loc] = stream.str();
            alreadySet[loc] = true;
          }
        }
      } else if (typeClass == TypeClass::String) {
        const std::vector<std::string> values = source.readAsVector<std::string>();
        for (size_t loc = 0; loc < nlocs; loc++) {
          if (!alreadySet[loc] && values[loc] != odb_missing_string) {
            stationIDs[loc] = values[loc];
            alreadySet[loc] = true;
          }
        }
      } else {
        throw eckit::NotImplemented("Station IDs may only be constructed from variables of type "
                                    "int or string. Variable '" + name + "' is of a different type",
                                    Here());
      }
    } else if (sourceParameters.wmoId.value() != boost::none) {
      const WmoIdSourceParameters &wmoIdParameters = *sourceParameters.wmoId.value();
      if (!og.vars.exists(wmoIdParameters.blockNumber) ||
          !og.vars.exists(wmoIdParameters.stationNumber))
        continue;

      const std::vector<int> blockNumbers =
          og.vars[wmoIdParameters.blockNumber].readAsVector<int>();
      const std::vector<int> stationNumbers =
          og.vars[wmoIdParameters.stationNumber].readAsVector<int>();
      std::ostringstream stream;
      for (size_t loc = 0; loc < nlocs; loc++) {
        if (!alreadySet[loc] &&
            blockNumbers[loc] != odb_missing_int && stationNumbers[loc] != odb_missing_int) {
          stream.str("");
          stream << std::setfill('0') << std::setw(2) << blockNumbers[loc]
                 << std::setfill('0') << std::setw(3) << stationNumbers[loc];
          stationIDs[loc] = stream.str();
          alreadySet[loc] = true;
        }
      }
    }
  }

  for (size_t loc = 0; loc < nlocs; loc++)
    if (!alreadySet[loc] && stationIDs[loc].empty())
      stationIDs[loc] = odb_missing_string;

  ioda::Variable v = og.vars[parameters_.destination];
  v.write(stationIDs);
}

// -----------------------------------------------------------------------------

ConcatenateVariablesTransform::ConcatenateVariablesTransform(
    const Parameters_ &transformParameters, const ODC_Parameters &,
    const OdbVariableCreationParameters &)
  : transformParameters_(transformParameters)
{}

void ConcatenateVariablesTransform::transform(ObsGroup &og) const
{
  const std::vector<std::string> &sourceNames = transformParameters_.sources.value();
  ASSERT(!sourceNames.empty());
  const size_t numSources = sourceNames.size();

  // Gather all source variables.
  std::vector<Variable> sources;
  sources.reserve(sourceNames.size());
  std::transform(sourceNames.begin(), sourceNames.end(), std::back_inserter(sources),
                 [&og](const std::string &name) {
                   Variable source = og.vars[name];
                   if (source.getType().getClass() != TypeClass::String)
                     throw eckit::UserError("All concatenated variables must be of type string. "
                                            "Variable '" + name + "' is not.", Here());
                   return source;
                  });

  // Gather the values of these variables.
  std::vector<std::vector<std::string>> sourceValues;
  sourceValues.reserve(sourceNames.size());
  std::transform(sources.begin(), sources.end(), std::back_inserter(sourceValues),
                 [&og](const Variable &source) { return source.readAsVector<std::string>(); });

  const size_t numElements = sourceValues.front().size();
  if (std::any_of(std::next(sourceValues.begin()), sourceValues.end(),
                  [numElements](const std::vector<std::string> &values)
                  { return values.size() != numElements; }))
    throw eckit::UserError("All variables to concatenate must be of the same size.", Here());

  // Concatenate these values.
  std::vector<std::string> concatenatedValues(numElements);
  for (size_t e = 0; e < numElements; ++e) {
    size_t concatenatedLength = 0;
    for (size_t s = 0; s < numSources; ++s)
      concatenatedLength += sourceValues[s][e].size();
    std::string &concatenatedValue = concatenatedValues[e];
    concatenatedValue.reserve(concatenatedLength);
    for (size_t s = 0; s < numSources; ++s)
      concatenatedValue += sourceValues[s][e];
  }

  // Create the destination variable.
  const std::vector<Variable> destinationDimScales = destinationDimensionScales(og, sources.front());
  // Retrieval of creation attributes and dimensions seems not to be implemented yet.
  const VariableCreationParameters varCreationParameters =
      sources.front().getCreationParameters(false /*doAtts?*/, false /*doDims?*/);
  ioda::Variable destination = og.vars.createWithScales<std::string>(
    transformParameters_.destination, destinationDimScales, varCreationParameters);

  // Store the concatenated strings in the destination variable.
  destination.write(concatenatedValues);
}

std::vector<Variable> ConcatenateVariablesTransform::destinationDimensionScales(
    const ObsGroup &og, const Variable &firstSource)
{
  // Identify all existing dimension scales.
  std::vector<std::string> allVarNames = og.vars.list();
  const std::list<Named_Variable> allDimScales = identifyDimensionScales(og.vars, allVarNames);
  // Identify the dimension scales associated with the first source variable.
  const std::vector<std::vector<Named_Variable>> namedSourceDimScales =
    firstSource.getDimensionScaleMappings(allDimScales);
  ASSERT_MSG(std::all_of(namedSourceDimScales.begin(), namedSourceDimScales.end(),
                         [](const auto &v) { return v.size() == 1; }),
             "All dimensions of concatenated variables are expected to have an attached dimension "
             "scale");
  // Gather these scales in a flat vector.
  std::vector<Variable> destinationDimScales;
  destinationDimScales.reserve(namedSourceDimScales.size());
  std::transform(namedSourceDimScales.begin(), namedSourceDimScales.end(),
                 std::back_inserter(destinationDimScales),
                 [](const std::vector<Named_Variable>& scales) { return scales.front().var; });
  return destinationDimScales;
}

}  // namespace ODC
}  // namespace Engines
}  // namespace ioda
