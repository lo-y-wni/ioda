/*
 * (C) Crown copyright 2024, Met Office
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 */

#include "ioda/Engines/ODC/ObsGroupTransform.h"

#include "ioda/Engines/ODC/ObsGroupTransformFactory.h"
#include "ioda/Engines/ODC/DataFromSQL.h"

#include <sstream>

namespace ioda {
namespace Engines {
namespace ODC {

namespace {
ObsGroupTransformMaker<CreateDateTimeTransform> createDateTimeMaker("create dateTime");
ObsGroupTransformMaker<CreateStationIdTransform> createStationIdMaker(
    "create stationIdentification");

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

}  // namespace ODC
}  // namespace Engines
}  // namespace ioda
