
To use with allure reporting follow steps
robot --listener allure_robotframework opm_rb
# robot --variable current_date:202402 --listener allure_robotframework opm_rb\EHS_D_TP.robot
allure generate --clean output/allure
allure open

Running process from current directory
#   robot -P . EHS_D_TP.robot
#   robot --variable current_date:202410  EHS_D_TP.robot
#   robot opm-autovalidation/opm_rb/EHS_D_TP.robot
