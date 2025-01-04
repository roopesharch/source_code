*** Settings ***
Library    Browser
Library    Collections
Library    String
Resource    OPM_EXTRACTION.robot
Resource    SOURCE_EXTRACTION.robot
Resource    DATA_PROCESSING.robot
Library    setup.py
Library    teradata_source.py
Library    Logic/Stem2.py
Library    Logic/Schedule_Attainment.py
Library    Logic/Lost_batches.py
Library    Logic/denodo_source.py
Library    stockouts.py
Library    SeleniumLibrary  timeout=10   implicit_wait=1.5   run_on_failure=Capture Element Screenshot
| Suite Setup | Set Library Search Order | Browser | SeleniumLibrary

*** Variables ***
${BROWSER}    chromium
${HEADLESS}    False
${wait}        45s
${config}    {}
${scorecardName}    Scorecard_placeholder
${kpiName}    KPI_placeholder
${bucket}    bucket_placeholder
${source}    source_placeholder
${brands}    []
${tiers}    []
${OPM_score}    {}
${column_names}    []
${final_dictionary}    {}
${final_data_frame}    {}

*** Test Cases ***

Setting Up Global Variables
    ${configCurrentDate}=    Evaluate    dict(${config})['filters']['date']
    Set Global Variable    ${current_date}    ${configCurrentDate}

    ${configScorecardName}=    Evaluate    dict(${config})['scorecardName']
    Set Global Variable    ${scorecardName}    ${configScorecardName}

    ${configBucket}=    Evaluate    dict(${config})['bucket']
    Set Global Variable    ${bucket}    ${configBucket}

    ${configKpiName}=    Evaluate    dict(${config})['kpiName']
    Set Global Variable    ${kpiName}    ${configKpiName}

    ${configSource}=    Evaluate    dict(${config})['source']
    Set Global Variable    ${source}    ${configSource}

    ${configColumnNames}=    Evaluate    dict(${config})['column_names']
    Set Global Variable    ${column_names}    ${configColumnNames}

#OPM Data Extraction for ${scorecardName} - ${kpiName} Metric
#    Login to opm till home page
#    Verify landing page and user profile
#
#    ${lowercase_scorecard_name}=    Convert To Lowercase    ${scorecardName}
#    Navigate to scorecard home page   //*[@id='homepage-${lowercase_scorecard_name}-title']    ${scorecardName}
#    Select date
#    Scrape score from opm  ${bucket}   ${kpiName}    ${scorecardName}    ${tiers}    ${brands}
#    Exit browser

Source Data extraction for ${scorecardName} - ${kpiName} Metric
    IF    '${source}' == 'PowerBI' and '${scorecardName}' == 'SCLT' and '${kpiName}' == 'On-Time Launch'
        Navigate to PowerBI dashboard
        Select Date in the NPI OTL PowerBI dashbaord
        Scrape NPI OTL score from source PowerBI dashboard
        Exit browser
    ELSE IF    '${source}' == 'Denodo' and '${scorecardName}' == 'Deliver' and '${kpiName}' == 'Severe Injury or Fatality - Precursor (SIF-P) Reporting'
        Get YTD and MTD Query from JSON file
        Get denodo score for SIF P reporting KPI from Deliver
    ELSE IF    '${source}' == 'Denodo' and '${scorecardName}' == 'Deliver' and '${kpiName}' == 'Severe Injury or Fatality - Controls (SIF-P Controls)'
        Get YTD and MTD Query from JSON file
        Get Denodo scores for SIP P Controls KPI from Deliver
    ELSE IF    '${source}' == 'Denodo' and '${scorecardName}' == 'Deliver' and '${kpiName}' == 'Severe Injury or Fatality (SIF)'
        Get YTD and MTD Query from JSON file
        Get Denodo score for SIF KPI from Deliver
    ELSE IF    '${source}' == 'Synapse' and '${scorecardName}' == 'Deliver' and '${kpiName}' == 'Stock Outs Deliver Related'
        Log To Console    To be Implemented Soon
    ELSE IF  '${source}' == 'Teradata' and '${scorecardName}' == 'SCLT' and '${kpiName}' == 'STEM 2 Trend'
        ${teradata_query}=    Evaluate    dict(${config})['teradata_query']
        ${selected_date}     get_current_date    month_in_word-year
        ${teradata_df}      teradata_query_result     ${teradata_query}
        ${source_dict}      calculate_ytd_and_cmonth_stem     ${teradata_df}     ${selected_date}
        ${final_dictionary}=   combine_dictionary    ${OPM_score}   ${source_dict}
        Set Global Variable   ${final_dictionary}

    ELSE IF  '${source}' == 'Teradata' and '${scorecardName}' == 'SCLT' and '${kpiName}' == 'Schedule Attainment'
        ${teradata_query}=    Evaluate    dict(${config})['teradata_query']
        ${selected_date}     get_current_date    month_in_word-year
        ${teradata_df}      teradata_query_result     ${teradata_query}
        ${source_dict}      calculate_ytd_and_cmonth_scheduleattainment     ${teradata_df}     ${selected_date}
        ${final_dictionary}=   combine_dictionary    ${OPM_score}   ${source_dict}
        Set Global Variable   ${final_dictionary}
    ELSE IF  '${source}' == 'Teradata' and '${scorecardName}' == 'Planning' and '${kpiName}' == 'Batch Standard Performance (Large Molecule)'
        ${teradata_query}=    Evaluate    dict(${config})['teradata_query']
        ${selected_date}     get_current_date    yearmonth
        ${teradata_df}      teradata_query_result     ${teradata_query}
        ${result_df}      calculate_ytd_cmonth_batch_std      ${teradata_df}     ${selected_date}
        ${final_dictionary}=   combine_dictionary    ${OPM_score}   ${result_df}
        Set Global Variable   ${final_dictionary}
    ELSE IF  '${source}' == 'Flatfile' and '${scorecardName}' == 'SCLT' and '${kpiName}' == 'Supplier Reliability DM'
         Log To Console    Implementation in progress
#        ${ytd}=    Evaluate    dict(${config})['query']['ytd_query']
#        ${mtd}=    Evaluate    dict(${config})['query']['mtd_query']
#        ${flat_file_data_frame}    connect_flatfile_azureDB      ${ytd}      ${mtd}
#        ${flat_file_score}       calculate_supplier_dm_ytd_mtd       ${flat_file_data_frame}
#        Log To Console     ${flat_file_score}
#        ${final_dictionary}=   combine_dictionary    ${OPM_score}   ${flat_file_score}
#        Set Global Variable   ${final_dictionary}
    ELSE IF  '${source}' == 'CDL' and '${scorecardName}' == 'SCLT' and '${kpiName}' == 'Stockouts'
        ${source_dict}      stockouts_source_data
        Log To Console    ${source_dict}
        ${final_dictionary}=   combine_dictionary    ${OPM_score}   ${source_dict}
        Set Global Variable   ${final_dictionary}
    ELSE
        ${final_dictionary}=    Evaluate    {}
    END

Aggregate And Visualize The Scores
    Aggregate scores into data frame
    Verify scores extracted & export to excel

