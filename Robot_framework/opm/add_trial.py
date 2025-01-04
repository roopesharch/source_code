# import os
# import sys
# import configparser
# BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__))).replace('\\', '/')
# # sys.path.append(BASE_DIR)
#
#
# def read_conf(session, key):
#     # print(session, key)
#     parser = configparser.ConfigParser()
#     # parser.read('C:/Users/RS60/PycharmProjects/OPM_validation/opm-autovalidation/opm_rb/config.ini')
#     parser.read(BASE_DIR + '/config.ini')
#     return parser[session][key]


# from source import read_config as rc
source_score = '100.00'
opm_score = '100.6'


source_score = source_score.split('%')[0] if '.' in str(source_score) and '.' in str(
                        opm_score) else str(round(float(source_score.split('%')[0])))

opm_score = opm_score if '.' in str(opm_score) and '.' in str(source_score) else str(
                        round(float(opm_score)))

print(source_score)
print(opm_score)

print(abs(float(source_score) - float(opm_score)))

tolerance = 0.5
if abs(float(source_score) - float(opm_score)) <= tolerance:
   print("yes")
else:
   print("wrong")
