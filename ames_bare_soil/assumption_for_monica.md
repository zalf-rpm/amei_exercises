## ApsimCampbell
- comp: ps = 2.63
- comp: pom = 1.3
- comp: soilConstituentNames = "Rocks", "OrganicMatter", "Sand", "Silt", "Clay", "Water", "Ice", "Air"
- exo: weather_AirPressure = 1013.25
- exo: waterBalance_Eo =  PotentialEvapotranspiration // daily potential evaporation
-- Eo = Potential soil evapotranspiration from soil surface
- exo: waterBalance_Eos = PotentialEvapotranspiration //potential evaporation
-- Eos = Potential soil evaporation from soil surface
- exo: waterBalance_Es = ActualEvaporation // actual evaporation
-- ES = Actual (realised) soil water evaporation
- exo: microClimate_CanopyHeight = 0 // or monica crop height, if there's a crop
- state: monica soiltemp = ApsimCampbell aveSoilTemp at layer 2+i

## BiomaSurfacePartonSoilSWATC
- comp: LagCoefficient = 0.8
- exo: DayLength = astronomicDayLenght

## BiomaSurfaceSWATSoilSWATC
- comp: LagCoefficient = 0.8
- exo: WaterEquivalentOfSnowPack = monica SnowWaterEquivalent (was 0)

## DSSAT_EPICST
- comp: ISWWAT = "Y"
- exo: SNOW = monica getSnowDepth()
- exo: DEPIR = dailySumIrrigationWater()
- exo: MULCHMASS = 0

## DSSAT_ST
- comp: ISWWAT = "Y"
- comp: MSALB = monica environmentParameters().p_Albedo

## MONICA 


## Simplace
- comp: cInitialAgeOfSnow = 0
- comp: SnowCoverCalculator.setcSnowIsolationFactorA = 2.3
- comp: SnowCoverCalculator.setcSnowIsolationFactorB(0.22);
- comp: cInitialSnowWaterContent = 0
- comp: cAlbedo = monica environmentParameters().p_Albedo
- comp: cDampingDepth = 6 // is also default
- exo: iPotentialSoilEvaporation = MONICA::get_ET0() * MONICA::params.pm_KcFactor
- exo: iCropResidues = 0
- exo: iSoilWaterContent = mmWcSum += sl.get_Vs_SoilMoisture_m3() * 10 * sl.vs_LayerThickness * 100

## SQ_Soil_Temperature
- comp: a = 0.5
- comp: b = 1.81
- comp: c = 0.49
- comp: lambda_ = 2.454
- exo: dayLength = monica::astronomicDayLenght
- rate: heatFlux = 0
- out: soilSurfaceTemperature = (state.minTSoil + state.maxTSoil)/2.0
- out: soil temp each layer = (state.minTSoil + state.maxTSoil)/2.0

## Stics
- exo: min_temp = tmin
- exo: max_temp = tmax
- exo: min_canopy_temp = tmin
- exo: max_canopy_temp = tmax
- exo: min_air_temp = tmin
- out: soilSurfaceTemperature = state.temp_profile[0]
