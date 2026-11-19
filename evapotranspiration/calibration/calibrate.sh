#!/usr/bin/env bash
# ET study (AgMIP Wheat ET2, phase 1): how the Bushland cultivar was calibrated (2026-11-18), in order.
#
# Reruns exactly the adopted calibration runs (same CSVs, seeds and repetitions), copies their results into
# the parameter files the study uses, and reruns the study with them. Why each step looks like this:
# evapotranspiration/FINDINGS.md, "Calibration". Takes about 15 minutes.
#
#     evapotranspiration/calibration/calibrate.sh             # the adopted runs
#     SEED_CHECK=1 evapotranspiration/calibration/calibrate.sh  # + the other seed of steps 1-2, to /tmp
#
# Principles: never calibrate against ET (it's what the study evaluates); no crop parameters that change
# MONICA's crop ET0 (MinimumTemperatureForAssimilation did, via the stomatal resistance, so it was dropped
# from step 2); two seeds per step; the setup is the study's (src/amei_exercises/evapotranspiration/
# site_bushland.py: 24 x 10 cm layers, N response on, Odin's standing water fixes, initial N spread by SOC,
# the anhydrous ammonia at its injection depth).
#
# Avignon is not calibrated (its data are being checked): run_et uses the default cultivar there.
set -euo pipefail
cd "$(dirname "$0")/../.."  # the repository

CAL=evapotranspiration/calibration
CROPS=evapotranspiration/phase_1/params/crops  # the study's PARAMS; includes are relative to its parent
F_TREATMENTS=(TXBL1990F TXBL1992F TXBL1993F)  # full irrigation: no water stress

seed_check() {  # the same step with seed 1 into /tmp, only for comparison
    [[ "${SEED_CHECK:-0}" == 1 ]] || return 0
    pixi run calibrate "$@" --out "/tmp/seed_check_$2_$4"  # calibrate.py's default seed 1
}
seed_check_7() {  # the same step with seed 7 into /tmp
    [[ "${SEED_CHECK:-0}" == 1 ]] || return 0
    pixi run calibrate "$@" --seed 7 --out "/tmp/seed_check_$2_$4"
}

# 1. Phenology: emergence, ear emergence (MONICA's stage 4), anthesis, maturity of all 6 treatments,
#    from the default winter wheat. Free: StageTemperatureSum 0, 3, 4, stages 1-2 by one factor,
#    VernalisationRequirement; the daylength requirement traded off with the temperature sums, so it
#    stays at its default. N response on (the site's). Seed 7 adopted; the two seeds agree.
pixi run calibrate --site bushland --step phenology --params-csv $CAL/bushland_1_phenology.csv \
    --cultivar crops/cultivar_winter-wheat.json --seed 7
seed_check --site bushland --step phenology --params-csv $CAL/bushland_1_phenology.csv \
    --cultivar crops/cultivar_winter-wheat.json
cp $CAL/bushland_phenology/cultivar.json $CROPS/cultivar_winter-wheat_bushland_1_phenology.json

# 2. Growth: daily LAI and above ground biomass of the F treatments (water for the P treatments would be
#    a later step). Free: MaxAssimilationRate, SpecificLeafArea by one factor, leaf senescence in stage 4
#    and from anthesis. N response off: the F treatments taken as not N-limited, as the experiment
#    presumably intended, so the crop parameters don't absorb MONICA's N uncertainty (no N observations);
#    the study's runs keep it on. Seed 1 adopted; the two seeds agree.
pixi run calibrate --site bushland --step growth --params-csv $CAL/bushland_2_growth.csv \
    --cultivar crops/cultivar_winter-wheat_bushland_1_phenology.json \
    --treatments "${F_TREATMENTS[@]}" --no-nitrogen-stress
seed_check_7 --site bushland --step growth --params-csv $CAL/bushland_2_growth.csv \
    --cultivar crops/cultivar_winter-wheat_bushland_1_phenology.json \
    --treatments "${F_TREATMENTS[@]}" --no-nitrogen-stress
cp $CAL/bushland_growth/cultivar.json $CROPS/cultivar_winter-wheat_bushland_2_growth.json

# 3. Yield: species AssimilateReallocation against the F treatments' yields. Checked, NOT adopted (RMSE
#    0.6 -> 0.5 t/ha only by trading 1993 against 1990/1992), so the study's cultivar is step 2's.
pixi run calibrate --site bushland --step yield --params-csv $CAL/bushland_3_yield.csv \
    --cultivar crops/cultivar_winter-wheat_bushland_2_growth.json \
    --treatments "${F_TREATMENTS[@]}" --no-nitrogen-stress --reps 500

# 4. The study's runs with the calibrated cultivar (site_bushland.py CULTIVAR = the step 2 file, N on),
#    and the comparison with the measured ET.
pixi run run-et --site bushland
pixi run evaluate-et --site bushland
pixi run run-et --site avignon
pixi run evaluate-et --site avignon
