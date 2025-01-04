*** Settings ***
Resource    OPM_EXTRACTION.robot

*** Keywords ***

Navigate to PowerBI dashboard
    TRY
        ${config_powerbi_dashboard_link}=    Evaluate    dict(${config})['powerbi_dashboard_link']
        Set Global Variable    ${powerbi_dashboard_link}    ${config_powerbi_dashboard_link}

        New Browser    browser=${BROWSER}    headless=${HEADLESS}   args=["--start-maximized","--incognito"]
        New Context    viewport=${None}
        Retry Opening Page    ${powerbi_dashboard_link}

        Wait For Elements State   xpath=//*[@class="pbi-text-input"]   visible   ${wait}
        ${user}   read_conf   opm   opm_user
        Type Text   xpath=//*[@class="pbi-text-input"]  ${user}

        Wait For Elements State   xpath=//button[contains(text(),'Submit')]   visible   ${wait}
        Click      xpath=//button[contains(text(),'Submit')]

        Wait For Elements State   xpath=//input[@id="idSIButton9"]   visible   ${wait}
        Click      xpath=//input[@id="idSIButton9"]

        ${config_page_title}=    Evaluate    dict(${config})['page_title']
        Set Global Variable    ${page_title}    ${config_page_title}

        Wait For Elements State    xpath=//span[contains(@class, "artifact-title") and text()="${page_title}"]   visible   ${wait}

        printf   WHITE   PowerBI dashboard loaded successfully
    EXCEPT
        printf   RED   Error accessing PowerBI dashboard
    END

Select Date in the NPI OTL PowerBI dashbaord
    ${CURRENT_YEAR}    Get Current Date    year
    ${CURRENT_MONTH}    Get Current Date    month

    # Opening the YTD filter
    Wait For Elements State    xpath=(//div[@aria-label="YYYY-MM"])   visible   ${wait}
    Click    xpath=(//div[@aria-label="YYYY-MM"])
    ${dropdown_status}=  Get Text   xpath=//div[@aria-label="YYYY-MM"]//div[@class="slicer-restatement"]
    TRY
        ${all_selected}=  Get Text   xpath=//div[@aria-label="YYYY-MM"]//div[@class="scrollRegion"]//div[@aria-selected="true"]//span[@class="slicerText"]
    EXCEPT
        ${all_selected}=    Set Variable    False
    END

    # Deselect all the previous selected options
    Click    xpath=//span[contains(text(),'Select all')]
    Sleep    0.5s

    # Conditional logic for number of clicks
    Run Keyword If    '${dropdown_status}' != 'All' or ('${dropdown_status}' == 'All' and '${all_selected}' != 'False' and '${all_selected}' != 'Select all')    Click    xpath=//span[contains(text(),'Select all')]
    Sleep    0.5s

    ${end_month}=    Evaluate    (int('${CURRENT_MONTH}') % 12) + 1
    ${dates}=    Create List
    FOR    ${i}    IN RANGE    1    ${end_month}
        ${month}=    Evaluate    str(${i}).zfill(2)
        ${date}=    Set Variable    ${CURRENT_YEAR}-${month}
        Append To List    ${dates}    ${date}
    END

    # Iterate through the list of dates and select them
    FOR    ${date}    IN    @{dates}
        # Check visibility and scroll if necessary
        ${states}=    Get Element States    xpath=//span[contains(text(),'${date}')]
        ${is_visible}=    Evaluate    'visible' in ${states}

        WHILE    $is_visible == False
            Press Keys    //div[@aria-label="YYYY-MM"]//div[@class="scrollRegion"]    ArrowDown
            ${states}=    Get Element States    xpath=//span[contains(text(),'${date}')]
            ${is_visible}=    Evaluate    'visible' in ${states}
        END

        Keyboard Key    down     Control
        # Press Keys    //span[contains(text(),'${date}')]    Enter
        Click    xpath=//span[contains(text(),'${date}')]
    END
    Sleep    0.5s

    # Opening the DC Launch Month filter
    Wait For Elements State    xpath=(//div[@aria-label="DC Launch Month"])   visible   ${wait}
    Click    xpath=(//div[@aria-label="DC Launch Month"])
    Sleep    0.5s

    # Scroll to the specific date in the dropdown
    Click    xpath=//span[contains(text(),'Select all')]
    Click    xpath=//span[contains(text(),'Select all')]
    ${states}=    Get Element States    xpath=//div[@aria-label="DC Launch Month"]//div[@class="scrollRegion"]//span[contains(text(),'${CURRENT_YEAR}-${CURRENT_MONTH}')]
    ${is_visible}=    Evaluate    'visible' in ${states}

    WHILE    $is_visible == False
        Press Keys    //div[@aria-label="DC Launch Month"]//div[@class="scrollRegion"]    ArrowDown
        ${states}=    Get Element States    xpath=//div[@aria-label="DC Launch Month"]//div[@class="scrollRegion"]//span[contains(text(),'${CURRENT_YEAR}-${CURRENT_MONTH}')]
        ${is_visible}=    Evaluate    'visible' in ${states}
    END

    Click    xpath=//div[@aria-label="DC Launch Month"]//div[@class="scrollRegion"]//span[contains(text(),'${CURRENT_YEAR}-${CURRENT_MONTH}')]
    Click    xpath=(//div[@aria-label="DC Launch Month"])[1]

    printf   WHITE   Date dropdown selected: ${CURRENT_YEAR}-${CURRENT_MONTH}
    Sleep    2s

Scrape NPI OTL score from source PowerBI dashboard
    ${otl_monthly_score}=    Get Text    xpath=//h3[(text()='% On-time')]/ancestor::div[contains(@class,'visualTitleArea')]/following-sibling::*[1]
    ${otl_ytd_score}=    Get Text    xpath=//h3[(text()='% On-time YTD')]/ancestor::div[contains(@class,'visualTitleArea')]/following-sibling::*[1]

    ${powerbi_score}=    Evaluate    {'LM & SM': ['${otl_ytd_score}', '${otl_monthly_score}']}

    TRY
        IF    '${OPM_score}[0]' == 'No Data'
            ${OPM_score}=    Evaluate    {'LM & SM': ['No Data', 'No Data']}
        END
    EXCEPT
        Log    message
    END
    ${final_dictionary}=   combine_dictionary    ${OPM_score}   ${powerbi_score}
    Set Global Variable   ${final_dictionary}

    printf   WHITE   Extracted PowerBI Dashboard Data:
    Log To Console   ${powerbi_score}
    printf   WHITE   ------------------------------------------------------------------------------

Get YTD and MTD Query from JSON file
    ${configYTDQuery}=    Evaluate    dict(${config})['query']['ytd_query']
    Set Global Variable    ${ytd_query}    ${configYTDQuery}

    ${configMTDQuery}=    Evaluate    dict(${config})['query']['mtd_query']
    Set Global Variable    ${mtd_query}    ${configMTDQuery}

Get denodo score for SIF P reporting KPI from Deliver
    FOR    ${main_key}    ${main_value}    IN    &{OPM_score}
            TRY
                IF    '${main_value}[0]' == 'No Data'
                Append To List    ${main_value}   No Data
                Append To List    ${main_value}   No Data
                CONTINUE
                END
            EXCEPT
                Log    message
            END

            FOR    ${sub_key}    ${sub_value}    IN    &{main_value}
                ${code}   get_site_id_and_propertycode_for_SIF_Deliver    ${sub_key}

                ${denodo_query_resut}    get_denodo_query_result    ${ytd_query}   ${mtd_query}   ${code}[0]   ${code}[1]
                IF  '${denodo_query_resut}[0]' == 'None' or '${denodo_query_resut}[1]' == 'None' or '${denodo_query_resut}[2]' == 'None' or '${denodo_query_resut}[3]' == 'None'
                    Append To List    ${sub_value}    N/A
                ELSE
                    ${ytd}   get_quotient    ${denodo_query_resut}[0]   ${denodo_query_resut}[1]
                    Append To List    ${sub_value}    ${ytd}
                END

                IF  '${denodo_query_resut}[2]' == 'None' or '${denodo_query_resut}[3]' == 'None'
                    Append To List    ${sub_value}    N/A
                ELSE
                    ${mtd}   get_quotient    ${denodo_query_resut}[2]   ${denodo_query_resut}[3]
                    Append To List    ${sub_value}    ${mtd}
                END
            END
    END

    ${final_dictionary}=    Set Variable    ${OPM_score}
    Set Global Variable     ${final_dictionary}

Get Denodo scores for SIP P Controls KPI from Deliver
    FOR    ${main_key}    ${main_value}    IN    &{OPM_score}
            TRY
                IF    '${main_value}[0]' == 'No Data'
                Append To List    ${main_value}   No Data
                Append To List    ${main_value}   No Data
                CONTINUE
                END

            EXCEPT
                Log    message
            END

            FOR    ${sub_key}    ${sub_value}    IN    &{main_value}
                ${code}   get_site_id_and_propertycode_for_SIF_Deliver    ${sub_key}

                ${denodo_query_resut}    get_denodo_query_result    ${ytd_query}   ${mtd_query}   ${code}[0]   ${code}[1]
                IF  '${denodo_query_resut}[0]' == 'None' or '${denodo_query_resut}[1]' == 'None' or '${denodo_query_resut}[2]' == 'None' or '${denodo_query_resut}[3]' == 'None'
                    Append To List    ${sub_value}    N/A
                ELSE
                    ${ytd}   get_percentage    ${denodo_query_resut}[0]   ${denodo_query_resut}[1]
                    Append To List    ${sub_value}    ${ytd}
                END

                IF  '${denodo_query_resut}[2]' == 'None' or '${denodo_query_resut}[3]' == 'None'
                    Append To List    ${sub_value}    N/A
                ELSE
                    ${mtd}   get_percentage    ${denodo_query_resut}[2]   ${denodo_query_resut}[3]
                    Append To List    ${sub_value}    ${mtd}
                END
            END
    END

    ${final_dictionary}=    Set Variable    ${OPM_score}
    Set Global Variable     ${final_dictionary}

Get Denodo score for SIF KPI from Deliver
    FOR    ${main_key}    ${main_value}    IN    &{OPM_score}
            TRY
                IF    '${main_value}[0]' == 'No Data'
                Append To List    ${main_value}   No Data
                Append To List    ${main_value}   No Data
                CONTINUE
                END

            EXCEPT
                Log    message
            END

            FOR    ${sub_key}    ${sub_value}    IN    &{main_value}
                ${code}   get_site_id_and_propertycode_for_SIF_Deliver    ${sub_key}

                ${denodo_query_resut}    get_denodo_query_result    ${ytd_query}   ${mtd_query}   ${code}[0]   ${code}[1]
                IF  '${denodo_query_resut}[0]' == 'None'
                    Append To List    ${sub_value}    N/A
                ELSE
                    Append To List    ${sub_value}    ${denodo_query_resut}[0]
                END

                IF  '${denodo_query_resut}[1]' == 'None'
                    Append To List    ${sub_value}    N/A
                ELSE
                    Append To List    ${sub_value}    ${denodo_query_resut}[1]

                END
            END
    END

    ${final_dictionary}=    Set Variable    ${OPM_score}
    Set Global Variable     ${final_dictionary}
