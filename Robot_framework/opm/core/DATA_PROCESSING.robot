*** Settings ***
Library    Browser
Library    setup.py
Library    Collections

*** Keywords ***

Aggregate scores into data frame
    printf   WHITE   Final aggregated data:

    ${configSource}=    Evaluate    dict(${config})['column_names']
    Set Global Variable    ${column_names}    ${configSource}

    IF    'Tier' in ${column_names} or 'Brand' in ${column_names}
        ${final_data_frame}=    write_multiple_json_to_dataframe    ${final_dictionary}   ${column_names}
    ELSE
        printf   WHITE   ${scorecardName}
        ${final_data_frame}=    write_dictionary_to_data_frame    ${final_dictionary}   ${column_names}
    END

    Set Global Variable   ${final_data_frame}

Verify scores extracted & export to excel
     printf   WHITE   Comparing final scores...

    IF    'Tier' in ${column_names} or 'Brand' in ${column_names}
        Log    Tier or Brand found
    ELSE
        ${config_expected_l2}=    Evaluate    dict(${config})['expected_l2']
        ${differences}=    Compare_lists     ${config_expected_l2}    ${final_dictionary}
    END


    IF    'Tier' in ${column_names} or 'Brand' in ${column_names}
        ${mismatch_score_cell_id}=    compare_multiple_kpi_score_df    ${final_data_frame}
    ELSE
        ${mismatch_score_cell_id}=    compare_scores    ${final_data_frame}
    END


    # Result export to excel
    printf   WHITE   Exporting final result...

    IF    'Tier' in ${column_names} or 'Brand' in ${column_names}
        write_multiple_KPI_df_to_excel_report    ${filePath}   ${mismatch_score_cell_id}[0]
    ELSE
        write_excel_file    ${filePath}   ${sheetName}    ${final_data_frame}    ${mismatch_score_cell_id}    ${differences}
    END

    printf   WHITE   Export completed: ${filePath} | ${sheetName}
    printf   WHITE   ------------------------------------------------------------------------------