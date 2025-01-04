
# Press Shift+F10 to execute it or replace it with your code.
# Press Double Shift to search everywhere for classes, files, tool windows, actions, and settings.
# Use the below line to create a build
# pyinstaller  main.py --onefile
from Score_reporting_automation import Read_stem_brand_data as Sbd
from Score_reporting_automation import  Read_stockout_data as Rsd
from source import install_packages as ip
import Read_otif_d_data as Rodd


# logger = stp.log('stockout_path')


def stock_out_qa_automation():

    # logger.info('Starting stockouts QA automation')
    # logger.debug("man debug Message")
    # logger.info("Just an information")
    # logger.warning("Its a Warning")
    # logger.error("Did you try to divide by zero")
    # logger.critical("Internet is down")
    # logger.info('Starting stockouts QA automation')
    final_data_dict = {}
    # logger.info('Initiating data read from kelvin UI')
    stock_out_kelvin_data = Rsd.Read_Kelvin_stockout_Data(final_data_dict)
    # logger.info('Completed reading kelvin UI')
    print(stock_out_kelvin_data.final_data_dict)
    # logger.info('Initiating data read from CV data')
    synapse_cv_output = Rsd.Read_cv_Synapse_stockout_Data(stock_out_kelvin_data.final_data_dict)
    # logger.info('Completed reading CV data from synapse')
    print(synapse_cv_output.final_data_dict)
    # logger.info('Initiating data read from Tera data')
    teradata_output = Rsd.Read_Teradata_stockout_Data(synapse_cv_output.final_data_dict)
    # logger.info('Completed reading Tera data')
    print(teradata_output.final_data_dict)
    # logger.info('Initiating data read from kevin synapse data')
    synapse_kelvin_output = Rsd.Read_kelvin_Synapse_stockout_Data(teradata_output.final_data_dict)
    # logger.info('Completed reading Kelvin Synapse data')
    Rsd.write_stock_out_to_data_frame(synapse_kelvin_output.final_data_dict)
    # logger.info('Completed reading reading and writing stockout data')


def otif_d_qa_automation():
    otif_d_kelvin_data = Rodd.Read_Kelvin_otif_d_Data({})
    print(otif_d_kelvin_data.final_data_dict)
    otif_d_pharma_data = Rodd.Read_otif_pharma_data(otif_d_kelvin_data.final_data_dict, otif_d_kelvin_data.driver)
    print(otif_d_pharma_data.final_data_dict)
    Rodd.write_otif_to_data_frame(otif_d_pharma_data.final_data_dict)

def brand_stem_qa_automation():
    brand_stem_kelvin_data = Sbd.Read_Kelvin_stem_brand_Data({})
    print(brand_stem_kelvin_data.final_data_dict)
    brand_stem_tera_data = Sbd.Read_Teradata_brand_stem_Data(brand_stem_kelvin_data.final_data_dict)
    print(brand_stem_tera_data.final_data_dict)
    Sbd.write_brand_stem_to_data_frame(brand_stem_tera_data.final_data_dict)
    # f1 = {'Akeega : # STEMS OPENED DURING THE PERIOD': ['0', '0', 0, 0], 'Akeega : # CURRENT ACTIVE STEMS': ['TBD', 'TBD', 0, 0], 'Balversa : # STEMS OPENED DURING THE PERIOD': ['1', '1', 1, 1], 'Balversa : # CURRENT ACTIVE STEMS': ['TBD', 'TBD', 0, 0], 'Carvykti : # STEMS OPENED DURING THE PERIOD': ['0', '0', 0, 0], 'Carvykti : # CURRENT ACTIVE STEMS': ['TBD', 'TBD', 0, 0], 'Concerta : # STEMS OPENED DURING THE PERIOD': ['3', '3', 4, 4], 'Concerta : # CURRENT ACTIVE STEMS': ['TBD', 'TBD', 1, 1], 'Darzalex : # STEMS OPENED DURING THE PERIOD': ['1', '1', 1, 1], 'Darzalex : # CURRENT ACTIVE STEMS': ['TBD', 'TBD', 0, 0], 'Darzalex Faspro : # STEMS OPENED DURING THE PERIOD': ['1', '1', 0, 0], 'Darzalex Faspro : # CURRENT ACTIVE STEMS': ['TBD', 'TBD', 0, 0], 'Ebola : # STEMS OPENED DURING THE PERIOD': ['0', '0', 0, 0], 'Ebola : # CURRENT ACTIVE STEMS': ['TBD', 'TBD', 0, 0], 'Edurant : # STEMS OPENED DURING THE PERIOD': ['0', '0', 0, 0], 'Edurant : # CURRENT ACTIVE STEMS': ['TBD', 'TBD', 0, 0], 'Elmiron : # STEMS OPENED DURING THE PERIOD': ['0', '0', 0, 0], 'Elmiron : # CURRENT ACTIVE STEMS': ['TBD', 'TBD', 0, 0], 'Erleada : # STEMS OPENED DURING THE PERIOD': ['2', '2', 2, 2], 'Erleada : # CURRENT ACTIVE STEMS': ['TBD', 'TBD', 1, 1], 'Imbruvica : # STEMS OPENED DURING THE PERIOD': ['6', '6', 6, 6], 'Imbruvica : # CURRENT ACTIVE STEMS': ['TBD', 'TBD', 2, 2], 'Invega : # STEMS OPENED DURING THE PERIOD': ['2', '2', 3, 3], 'Invega : # CURRENT ACTIVE STEMS': ['TBD', 'TBD', 0, 0], 'Invega Hafyera : # STEMS OPENED DURING THE PERIOD': ['1', '1', 0, 0], 'Invega Hafyera : # CURRENT ACTIVE STEMS': ['TBD', 'TBD', 0, 0], 'Invega Sustenna : # STEMS OPENED DURING THE PERIOD': ['1', '1', 1, 1], 'Invega Sustenna : # CURRENT ACTIVE STEMS': ['TBD', 'TBD', 0, 0], 'Invega Trinza : # STEMS OPENED DURING THE PERIOD': ['1', '1', 0, 0], 'Invega Trinza : # CURRENT ACTIVE STEMS': ['TBD', 'TBD', 0, 0], 'Invokana : # STEMS OPENED DURING THE PERIOD': ['4', '4', 4, 4], 'Invokana : # CURRENT ACTIVE STEMS': ['TBD', 'TBD', 2, 2], 'Opsumit : # STEMS OPENED DURING THE PERIOD': ['1', '1', 1, 1], 'Opsumit : # CURRENT ACTIVE STEMS': ['TBD', 'TBD', 1, 1], 'Ponvory : # STEMS OPENED DURING THE PERIOD': ['0', '0', 0, 0], 'Ponvory : # CURRENT ACTIVE STEMS': ['TBD', 'TBD', 0, 0], 'Prezcobix : # STEMS OPENED DURING THE PERIOD': ['2', '2', 1, 1], 'Prezcobix : # CURRENT ACTIVE STEMS': ['TBD', 'TBD', 0, 0], 'Prezista : # STEMS OPENED DURING THE PERIOD': ['2', '2', 1, 1], 'Prezista : # CURRENT ACTIVE STEMS': ['TBD', 'TBD', 0, 0], 'Rekambys/Cabenuva : # STEMS OPENED DURING THE PERIOD': ['0', '0', 0, 0], 'Rekambys/Cabenuva : # CURRENT ACTIVE STEMS': ['TBD', 'TBD', 0, 0], 'Remicade : # STEMS OPENED DURING THE PERIOD': ['0', '0', 0, 0], 'Remicade : # CURRENT ACTIVE STEMS': ['TBD', 'TBD', 0, 0], 'Risperdal : # STEMS OPENED DURING THE PERIOD': ['3', '3', 8, 8], 'Risperdal : # CURRENT ACTIVE STEMS': ['TBD', 'TBD', 2, 2], 'Risperdal Consta : # STEMS OPENED DURING THE PERIOD': ['1', '1', 2, 2], 'Risperdal Consta : # CURRENT ACTIVE STEMS': ['TBD', 'TBD', 0, 0], 'Rybrevant : # STEMS OPENED DURING THE PERIOD': ['2', '2', 2, 2], 'Rybrevant : # CURRENT ACTIVE STEMS': ['TBD', 'TBD', 0, 0], 'Simponi : # STEMS OPENED DURING THE PERIOD': ['0', '0', 0, 0], 'Simponi : # CURRENT ACTIVE STEMS': ['TBD', 'TBD', 0, 0], 'Sirturo : # STEMS OPENED DURING THE PERIOD': ['0', '0', 0, 0], 'Sirturo : # CURRENT ACTIVE STEMS': ['TBD', 'TBD', 0, 0], 'Spravato : # STEMS OPENED DURING THE PERIOD': ['2', '2', 2, 2], 'Spravato : # CURRENT ACTIVE STEMS': ['TBD', 'TBD', 2, 2], 'Stelara : # STEMS OPENED DURING THE PERIOD': ['0', '0', 1, 1], 'Stelara : # CURRENT ACTIVE STEMS': ['TBD', 'TBD', 0, 0], 'Symtuza : # STEMS OPENED DURING THE PERIOD': ['2', '2', 0, 0], 'Symtuza : # CURRENT ACTIVE STEMS': ['TBD', 'TBD', 0, 0], 'Talvey : # STEMS OPENED DURING THE PERIOD': ['0', '0', 0, 0], 'Talvey : # CURRENT ACTIVE STEMS': ['TBD', 'TBD', 0, 0], 'Tecvayli : # STEMS OPENED DURING THE PERIOD': ['1', '1', 0, 0], 'Tecvayli : # CURRENT ACTIVE STEMS': ['TBD', 'TBD', 0, 0], 'Topamax : # STEMS OPENED DURING THE PERIOD': ['4', '4', 4, 4], 'Topamax : # CURRENT ACTIVE STEMS': ['TBD', 'TBD', 1, 1], 'Tracleer : # STEMS OPENED DURING THE PERIOD': ['1', '1', 1, 1], 'Tracleer : # CURRENT ACTIVE STEMS': ['TBD', 'TBD', 0, 0], 'Tremfya : # STEMS OPENED DURING THE PERIOD': ['0', '0', 0, 0], 'Tremfya : # CURRENT ACTIVE STEMS': ['TBD', 'TBD', 0, 0], 'Uptravi : # STEMS OPENED DURING THE PERIOD': ['0', '0', 0, 0], 'Uptravi : # CURRENT ACTIVE STEMS': ['TBD', 'TBD', 0, 0], 'Velcade : # STEMS OPENED DURING THE PERIOD': ['3', '3', 3, 3], 'Velcade : # CURRENT ACTIVE STEMS': ['TBD', 'TBD', 1, 1], 'Xarelto : # STEMS OPENED DURING THE PERIOD': ['0', '0', 1, 1], 'Xarelto : # CURRENT ACTIVE STEMS': ['TBD', 'TBD', 0, 0], 'Zytiga : # STEMS OPENED DURING THE PERIOD': ['0', '0', 0, 0], 'Zytiga : # CURRENT ACTIVE STEMS': ['TBD', 'TBD', 0, 0]}
    # Sbd.write_brand_stem_to_data_frame(f1)


if __name__ == '__main__':
    # Install python packages or dependencies from the file called 'install+packages.py'
    ip.InstallPackages()
    # logger.info("Installed all the dependencies and packages")
    from datetime import datetime
    now = datetime.now()
    print(now)

    


    # stp.set_month_and_year_to_confluence()
    # stock_out_qa_automation()
    # otif_d_qa_automation()
    # brand_stem_qa_automation()



    # '2024-02-08 14:35' time for reference
    time_to_run = ['2024-02-15 22:30']
    # for i in time_to_run:
    #     while True:
    #         now = datetime.now()
    #         if str(i) in str(now):
    #             print("Current Run Time", now)
    #             stp.set_month_and_year_to_confluence()
    #             # stock_out_qa_automation()
    #             otif_d_qa_automation()
    #             break














