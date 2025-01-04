** Settings ***
Library    Browser
Library    setup.py
Library    Collections
Library    String

*** Keywords ***

Retry Opening Page
    [Arguments]    ${url}
    ${RETRY_COUNT}=    read_conf    configurations    max_retry_attempts
    ${retry}=    Set Variable    ${RETRY_COUNT}
    printf   WHITE   ------------------------------------------------------------------------------

    WHILE    ${retry} > 0
        TRY
            New Page    ${url}
            printf    WHITE    Page (${url}) opened successfully!
            Exit For Loop
        EXCEPT
            printf    YELLOW    Failed to open the page (${url}). Retrying... (${RETRY_COUNT - ${retry} + 1} attempt)
            ${retry}=    Evaluate    ${retry} - 1
            Sleep    ${wait}
        END
    END

    Run Keyword If    ${retry} == 0    Fail    Failed to open page: ${url} after ${RETRY_COUNT} attempts

Login to opm till home page
    New Browser    browser=${BROWSER}    headless=${HEADLESS}   args=["--start-maximized","--incognito"]
    New Context    viewport=${None}

    ${opm_home_url}   read_conf   opm   opm_home_url
    Retry Opening Page    ${opm_home_url}

    ${xpath}    Set Variable    //button[contains(text(),'Sign in with JnJ account')]
    Wait For Elements State   xpath=${xpath}   visible   ${wait}
    Click      xpath=${xpath}

    ${xpath}    Set Variable  //*[@name="loginfmt"]
    Wait For Elements State   xpath=${xpath}   visible   ${wait}
    ${user}   read_conf   opm   opm_user
    Type Text   xpath=//*[@name="loginfmt"]  ${user}

    click   xpath=//*[@data-report-event="Signin_Submit"]

Verify landing page and user profile
    Wait For Elements State   xpath=//button[@id='sidebar-project-icon-home-link']   visible   ${wait}
    printf   WHITE   OPM dashboard loaded successfully
    
    ${xpath}  Set Variable   //div[@id='header-profile-avatar']
    Wait For Elements State   xpath=${xpath}   visible   ${wait}
    printf   WHITE   User profile available

Navigate to scorecard home page
    [Arguments]  ${home_page_scorecard_title}    ${scorecardName}
    Wait For Elements State   xpath=${home_page_scorecard_title}  visible   ${wait}
    click  xpath=${home_page_scorecard_title}
    printf   WHITE   Navigated to ${scorecardName} home page

Select date
    # get_current_date function from setup.py file to get date
    ${year}    get_current_date     year
    ${month}    get_current_date    month_in_word

    TRY
        Wait For Elements State    xpath=(//button[contains(@aria-label,'Choose date')])[1]   visible   ${wait}
        click  xpath=(//button[contains(@aria-label,'Choose date')])[1]

        Wait For Elements State    xpath=//button[contains(text(),'${year}')]   visible   ${wait}
        click  xpath=//button[contains(text(),'${year}')]

        Wait For Elements State    xpath=(//button[contains(@aria-label,'Choose date')])[2]   visible   2
        click  xpath=(//button[contains(@aria-label,'Choose date')])[2]

        Wait For Elements State    xpath=//button[contains(text(),'${month}')]   visible   ${wait}
        click  xpath=//button[contains(text(),'${month}')]
    EXCEPT
        Wait For Elements State    xpath=//button[contains(text(),'${month}')]   visible   ${wait}
        click  xpath=//button[contains(text(),'${month}')]
        printf   RED   Single calender used (Old Calender)

    END

    printf   WHITE   Selected Date: ${month}-${year}

Scrape score from opm
    [Arguments]   ${bucket}   ${kpiName}    ${scorecardName}    ${tiers}    ${brands}

    IF    'Brand' not in ${column_names}
        ${xpath}  Set Variable   //button[contains(@class,'MuiTab-root')]/div[text()='${bucket}']
        Wait For Elements State    xpath=${xpath}   visible   ${wait}
        click  xpath=${xpath}

        Wait For Elements State    xpath=//button[contains(@class,'Mui-selected')]/div[text()='${bucket}']     visible   ${wait}
        printf   WHITE   Selected Bucket: ${bucket} 
        Sleep    1s

        printf   WHITE   Extracting KPI: ${kpiName}
    END

    IF    'Tier' in ${column_names}
        ${OPM_score}=    Create Dictionary

        ${tier_list}=    Evaluate    ${tiers}
        FOR    ${tier}    IN    @{tier_list}
            Click   xpath=//input[contains(@placeholder,'Select Tier')]
            Sleep    1s
            ${xpath}  Set Variable   //ul[contains(@class,'MuiAutocomplete-listbox')]/li[contains(text(),'${tier}')][1]
            Wait For Elements State    xpath=${xpath}   visible   ${wait}
            Click    xpath=${xpath}
            Sleep    1s

            ${extracted_score}   Get scores from KPI    ${kpiName}
            ${Score_name}   Set Variable   ${tier}:${kpiName}
            Set To Dictionary    ${OPM_score}    ${Score_name}=${extracted_score}
        END
    ELSE IF    'Brand' in ${column_names}
        ${OPM_score}=    Create Dictionary

        ${brand_list}=    Evaluate    ${brands}
        FOR    ${brand}    IN    @{brand_list}
            Click   xpath=//span[@class='select-brand']/following-sibling::div
            Sleep    1s
            ${xpath}  Set Variable   //div[contains(text(),'${brand}')]
            Wait For Elements State    xpath=${xpath}   visible   ${wait}
            Click    xpath=${xpath}
            Sleep    1s

            ${xpath}  Set Variable   //button[contains(@class,'MuiTab-root')]/div[text()='${bucket}']
            Wait For Elements State    xpath=${xpath}   visible   ${wait}
            click  xpath=${xpath}

            Wait For Elements State    xpath=//button[contains(@class,'Mui-selected')]/div[text()='${bucket}']     visible   ${wait}
            Sleep    1s

            ${extracted_score}   Get scores from KPI    ${kpiName}
            ${Score_name}   Set Variable   ${brand}:${kpiName}
            Set To Dictionary    ${OPM_score}    ${Score_name}=${extracted_score}
        END
    ELSE
        ${OPM_score}=    Create Dictionary
        Set Global Variable    ${OPM_score}
        Wait For Elements State    xpath=//span[text()='${kpiName}']/ancestor-or-self::div[contains(@class,'MuiPaper-root')]      visible   ${wait}
        ${raw_score}=  Get Text   xpath=//span[text()='${kpiName}']/ancestor-or-self::div[contains(@class,'MuiPaper-root')]
        Sleep    1s
        ${OPM_score}=    get_scores    ${raw_score}
    END

    printf   WHITE   Extracted OPM Data:
    Set Global Variable     ${OPM_score}
    Log To Console    ${OPM_score}
    printf   WHITE   ------------------------------------------------------------------------------

Exit browser
    Close Context
    Close Browser

Get scores from KPI
    [Arguments]   ${kpiName}
    ${xpath}  Set Variable     //span[normalize-space(text())='${kpiName}']/ancestor-or-self::div[contains(@class,'MuiPaper-root')]
    Wait For Elements State    xpath=${xpath}     visible   ${wait}
    ${raw_score}=   Get Text   xpath=${xpath}
    ${OPM_score}    get_scores    ${raw_score}
    RETURN     ${OPM_score}