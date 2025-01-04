*** Settings ***
#Library    Browser   auto_closing_level=keep  enable_presenter_mode=True
Library    Browser
Library    setup.py
Library    Collections

*** Keywords ***

login to opm till home page

    ${BROWSER}   Set Variable   read_conf   opm   browser
    ${HEADLESS}   Set Variable   read_conf   opm   headless
    Log To Console    ${BROWSER}
    Log To Console    Yes fail
    New Browser    browser=${BROWSER}    headless=${HEADLESS}   args=["--start-maximized","--incognito"]
    New Context    viewport=${None}

#   Open OPM Home page in a New Tab
    ${opm_home_url}   read_conf   opm   opm_home_url
    New Page    ${opm_home_url}

    ${xpath}    Set Variable    //button[contains(text(),'Sign in with JnJ account')]
    Wait For Elements State   xpath=${xpath}   visible   ${wait}
    Click      xpath=${xpath}

    ${xpath}    Set Variable  //*[@name="loginfmt"]
    Wait For Elements State   xpath=${xpath}   visible   ${wait}
    ${user}   read_conf   opm   opm_user
    Type Text   xpath=//*[@name="loginfmt"]  ${user}

    click   xpath=//*[@data-report-event="Signin_Submit"]
    Sleep                               4s

verify landing page and user profile
    Wait For Elements State   xpath=//button[@id='sidebar-project-icon-home-link']   visible   ${wait}
    printf   'YELLOW'   OUTPUT
    printf   WHITE   OPM landing page loaded successfully
    ${xpath}  Set Variable   //div[@id='header-profile-avatar']
    Wait For Elements State   xpath=${xpath}   visible   ${wait}


navigate to scorecard home page
#    ${xpath}    Set Variable  //button[@id='sidebar-project-icon-home-link']
    [Arguments]  ${home_page_scorecard_title}   ${scorecard_title}
    Wait For Elements State   xpath=${home_page_scorecard_title}  visible   ${wait}
#    Wait For Elements State   xpath=//*[@id='homepage-deliver-title']  visible   ${wait}
    click  xpath=${home_page_scorecard_title}
#    Log To Console    Navigated to Deliver home page
    printf   WHITE   Navigated to ${scorecard_title} home page



Select date

    # get_current_date function from setup.py file to get date
    ${year}    get_current_date     year
    ${month}    get_current_date    month_in_word

    Log To Console     ${month} ${year}

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

    Sleep                               4s

click the bucket
    [Arguments]   ${bucket}
    ${xpath}  Set Variable   //button[contains(@class,'MuiTab-root')]/div[text()='${bucket}']
    Wait For Elements State    xpath=${xpath}   visible   ${wait}
    click  xpath=${xpath}

    Wait For Elements State    xpath=//button[contains(@class,'Mui-selected')]/div[text()='${bucket}']     visible   ${wait}
#    Log To Console    CUstomer  bucket selected
    printf   WHITE   CUstomer bucket selected

get scores from KPI
    [Arguments]   ${scorecard}
    ${xpath}  Set Variable     //span[normalize-space(text())='${scorecard}']/ancestor-or-self::div[contains(@class,'MuiPaper-root')]
#    Log To Console    ${xpath}
        Wait For Elements State    xpath=${xpath}     visible   ${wait}
        ${raw_score}=   Browser.Get Text   xpath=${xpath}
        ${OPM_score}    get_scores    ${raw_score}
        RETURN     ${OPM_score}