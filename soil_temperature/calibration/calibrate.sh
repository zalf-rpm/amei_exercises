#!/usr/bin/env bash
# Soil temperature study (AMEI): how the cultivars of the two sites came about (2026-11-18/20), in order.
#
# - Maricopa: calibrated here, independently of the Maricopa flows (maricopa_wheat_face/), from the
#   default spring wheat. Reruns exactly the adopted runs (same CSVs, seeds and repetitions), copies their
#   results into soil_temperature/params/crops/, reruns the study and compares with the flows' cultivar.
#   Why each step looks like this: soil_temperature/FINDINGS.md, "Maricopa calibration".
# - Bushland: not calibrated here; it uses the ET study's Bushland cultivar
#   (evapotranspiration/calibration/calibrate.sh), in this study's setup (48 x 5 cm layers, this
#   workbook's initial N and fertilization).
#
# Takes about an hour.
#
# Steps 1-3 (2026-11-18) ran without the previous crop's residue, which the setup has since 2026-11-19
# (FINDINGS.md, "Things MONICA can't represent" 1): a rerun now includes it and gives slightly different
# parameters (the residue's effect on the calibrated crop is small, see there). Steps 4-6 ran with it.
#
#     soil_temperature/calibration/calibrate.sh             # the adopted runs
#     SEED_CHECK=1 soil_temperature/calibration/calibrate.sh  # + seed 7 of each step, to /tmp
#
# Principles as for the ET study: not against ET, no crop parameters that change MONICA's crop ET0, two
# seeds per step. Setup: src/amei_exercises/soil_temperature/site_maricopa.py (42 x 5 cm layers, N response
# on, FACE CO2, buried drip irrigation, Odin's standing water fixes). The runs are extended past the
# observed harvest until maturity (calibrate.py).
set -euo pipefail
cd "$(dirname "$0")/../.."  # the repository

CAL=soil_temperature/calibration
CROPS=soil_temperature/params/crops  # the site's PARAMS; includes are relative to its parent
CONTROLS=(902 906 910 914)  # wet, high N, ambient CO2, one per season

seed_check() {  # the same step with seed 7 into /tmp, only for comparison
    [[ "${SEED_CHECK:-0}" == 1 ]] || return 0
    pixi run calibrate "$@" --seed 7 --out "/tmp/seed_check_$2_$4"
}

# 1. Phenology: emergence, ear emergence (MONICA's stage 4), anthesis, maturity of all 16 treatments
#    (64 dates), from the default spring wheat. Free: StageTemperatureSum 0, 3, 4, stages 1-2 by one
#    factor. The daylength requirement free gave the same RMSE, so it isn't calibrated.
pixi run calibrate --site maricopa --step phenology --params-csv $CAL/maricopa_1_phenology.csv \
    --cultivar crops/cultivar_spring-wheat_default.json
seed_check --site maricopa --step phenology --params-csv $CAL/maricopa_1_phenology.csv \
    --cultivar crops/cultivar_spring-wheat_default.json
cp $CAL/maricopa_phenology/cultivar.json $CROPS/cultivar_spring-wheat_maricopa_1_phenology.json

# 2. Growth: daily LAI, above ground biomass and green leaf weight (WLVG) of the controls. Free:
#    MaxAssimilationRate, SpecificLeafArea early/late, leaf shares of stages 2 and 3, leaf senescence,
#    the species' InitialOrganBiomass (the young crop was too big, so its N was diluted). N response on.
pixi run calibrate --site maricopa --step growth --params-csv $CAL/maricopa_2_growth.csv \
    --cultivar crops/cultivar_spring-wheat_maricopa_1_phenology.json --treatments "${CONTROLS[@]}"
seed_check --site maricopa --step growth --params-csv $CAL/maricopa_2_growth.csv \
    --cultivar crops/cultivar_spring-wheat_maricopa_1_phenology.json --treatments "${CONTROLS[@]}"
cp $CAL/maricopa_growth/cultivar.json $CROPS/cultivar_spring-wheat_maricopa_2_growth.json
cp $CAL/maricopa_growth/species.json $CROPS/species_wheat_maricopa_2_growth.json

# 3. Yield: the ear's share of the assimilates in stage 4 against the controls' yields (the harvest
#    index was too high).
pixi run calibrate --site maricopa --step yield --params-csv $CAL/maricopa_3_yield.csv \
    --cultivar crops/cultivar_spring-wheat_maricopa_2_growth.json \
    --species crops/species_wheat_maricopa_2_growth.json --treatments "${CONTROLS[@]}" --reps 500
seed_check --site maricopa --step yield --params-csv $CAL/maricopa_3_yield.csv \
    --cultivar crops/cultivar_spring-wheat_maricopa_2_growth.json \
    --species crops/species_wheat_maricopa_2_growth.json --treatments "${CONTROLS[@]}" --reps 500
cp $CAL/maricopa_yield/cultivar.json $CROPS/cultivar_spring-wheat_maricopa_3_yield.json

# 4. Water (2026-11-19): the crop coefficients (StageKcFactor, stages 2-5) against the measured ET per
#    period and the soil water in 20-200 cm, all 16 treatments, from the step 3 cultivar. MONICA's
#    water demand was ~60 % of the measured ET, so the dry treatments got no water stress (FINDINGS.md,
#    model issue 2). Against ET: only in this study, where ET isn't what's evaluated. Seeds agree.
pixi run calibrate --site maricopa --step water --params-csv $CAL/maricopa_4_water.csv \
    --cultivar crops/cultivar_spring-wheat_maricopa_3_yield.json \
    --species crops/species_wheat_maricopa_2_growth.json
seed_check --site maricopa --step water --params-csv $CAL/maricopa_4_water.csv \
    --cultivar crops/cultivar_spring-wheat_maricopa_3_yield.json \
    --species crops/species_wheat_maricopa_2_growth.json
# growth and yield again from step 1's cultivar, now with step 4's crop coefficients
pixi run python -c "
import json
c = json.load(open('$CROPS/cultivar_spring-wheat_maricopa_1_phenology.json'))
c['StageKcFactor'] = json.load(open('$CAL/maricopa_water/cultivar.json'))['StageKcFactor']
open('$CROPS/cultivar_spring-wheat_maricopa_4_water.json', 'w').write(json.dumps(c, indent=2) + '\\n')
"

# 5. Growth again (2026-11-20), as step 2, from the step 4 cultivar and the default species.
pixi run calibrate --site maricopa --step growth --params-csv $CAL/maricopa_5_growth.csv \
    --cultivar crops/cultivar_spring-wheat_maricopa_4_water.json --treatments "${CONTROLS[@]}" \
    --out $CAL/maricopa_growth_kc
seed_check --site maricopa --step growth --params-csv $CAL/maricopa_5_growth.csv \
    --cultivar crops/cultivar_spring-wheat_maricopa_4_water.json --treatments "${CONTROLS[@]}"
cp $CAL/maricopa_growth_kc/cultivar.json $CROPS/cultivar_spring-wheat_maricopa_5_growth.json
cp $CAL/maricopa_growth_kc/species.json $CROPS/species_wheat_maricopa_5_growth.json

# 6. Yield again, as step 3. The study's parameters: the step 6 cultivar with the step 5 species.
pixi run calibrate --site maricopa --step yield --params-csv $CAL/maricopa_6_yield.csv \
    --cultivar crops/cultivar_spring-wheat_maricopa_5_growth.json \
    --species crops/species_wheat_maricopa_5_growth.json --treatments "${CONTROLS[@]}" --reps 500 \
    --out $CAL/maricopa_yield_kc
seed_check --site maricopa --step yield --params-csv $CAL/maricopa_6_yield.csv \
    --cultivar crops/cultivar_spring-wheat_maricopa_5_growth.json \
    --species crops/species_wheat_maricopa_5_growth.json --treatments "${CONTROLS[@]}" --reps 500
cp $CAL/maricopa_yield_kc/cultivar.json $CROPS/cultivar_spring-wheat_maricopa_6_yield.json

# 7. All 16 treatments: the calibrated parameters (steps 1-3, and 1 + 4-6) vs the flows' (phase 1: the
#    flows' cultivar with calibrated phenology; phase 2: the flows' best.csv of the LAI step applied to
#    it, built by hand as flow_phase_2_cultivar.json). The tables in FINDINGS.md.
pixi run evaluate-maricopa --cultivar crops/cultivar_spring-wheat_maricopa_6_yield.json \
    --species crops/species_wheat_maricopa_5_growth.json --out /tmp/maricopa_eval/calibrated
pixi run evaluate-maricopa --cultivar crops/cultivar_spring-wheat_maricopa_3_yield.json \
    --species crops/species_wheat_maricopa_2_growth.json --out /tmp/maricopa_eval/calibrated_steps_1_3
pixi run evaluate-maricopa --cultivar crops/cultivar_spring-wheat_with_1_calibrated_phenology.json \
    --out /tmp/maricopa_eval/flow_phase_1
pixi run evaluate-maricopa --cultivar $CAL/flow_phase_2_cultivar.json --out /tmp/maricopa_eval/flow_phase_2

# 8. The study's runs (site_maricopa.py: the step 6 cultivar with the step 5 species; site_bushland.py:
#    the ET study's step 2 cultivar) and the models against each other.
pixi run run-st --site maricopa
pixi run run-st --site bushland
pixi run evaluate-st --site maricopa
pixi run evaluate-st --site bushland
