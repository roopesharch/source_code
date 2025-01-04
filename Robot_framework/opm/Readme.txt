
To use with allure reporting follow steps
robot --listener allure_robotframework opm_rb
# robot --variable current_date:202402 --listener allure_robotframework opm_rb\EHS_D_TP.robot
allure generate --clean output/allure
allure open

Running process from current directory
#   robot -P . EHS_D_TP.robot
#   robot --variable current_date:202410  EHS_D_TP.robot
#   robot opm-autovalidation/opm_rb/EHS_D_TP.robot



Current command that I'm using to create the executable file:
pyinstaller --name "QA AUTOVALIDATION" --manifest manifest.xml manage.py --onefile --add-data "C:/Users/AJena10/AppData/Local/Programs/Python/Python313/Lib/site-packages/robot;robot" --add-data "C:/Users/AJena10/AppData/Local/Programs/Python/Python313/Lib/site-packages/Browser;Browser" --add-data "C:/Users/AJena10/AppData/Local/Programs/Python/Python313/Lib/site-packages/teradatasql;teradatasql" --hidden-import Browser --hidden-import SeleniumLibrary
