# developer: JP
# ENV VARS:
# CV_PROJECT=
# CV_TOPIC=
# DLP_PROJECT=
# DLP_TEMPLATE=

from random import choice,randint,randrange
import string, uuid
import names
import json, os
from datetime import datetime, date, timedelta
from google.cloud import pubsub_v1
import google.cloud.dlp

project_id = os.environ.get('CV_PROJECT', 'CV_PROJECT environment variable is not set.') # "lt-aps-sbx"
topic_id = os.environ.get('CV_TOPIC', 'CV_TOPIC environment variable is not set.') # "add_new_patient"

dlp_project_id=os.environ.get('DLP_PROJECT', 'CV_PROJECT environment variable is not set.') # "lt-aps-sbx"
dlp_template_id=os.environ.get('DLP_TEMPLATE', 'CV_PROJECT environment variable is not set.') # cv-dlp-poc-v2"

def random_phone():
    return str(randint(4000000000, 9000000000))

def random_name():
    return [names.get_first_name(), names.get_last_name()]

def random_dob():
    start_date = date(1930, 1, 1)
    end_date = date(2020, 1, 1)
    time_between_dates = end_date - start_date
    days_between_dates = time_between_dates.days
    random_number_of_days = randrange(days_between_dates)
    random_date = start_date + timedelta(days=random_number_of_days)
    return random_date

def random_postal():
    postal1 = choice('KLMNP') #choice('KLM')
    postal2 = choice('123456789') #str(randint(0, 9))
    postal3 = choice('ABCDEFGHIJKLMNOPQRSTUVWYZ') #choice('ABCEGHJKLMNPRSTVWXYZ')
    postal4 = choice('0123456789') #str(randint(0, 9))
    postal5 = choice('ABCDEFGHIJKLMNOPQRSTUVWYZ') #choice('ABCEGHJKLMNPRSTVWXYZ')
    postal6 = choice('0123456789') #str(randint(0, 9))
    return postal1 + postal2 + postal3 + postal4 + postal5 + postal6

def random_regcode():
    code1 = choice('ABCDEFGHIJKLMNOPQRSTUVWYZ1234567890')
    code2 = choice('ABCDEFGHIJKLMNOPQRSTUVWYZ1234567890')
    code3 = choice('ABCDEFGHIJKLMNOPQRSTUVWYZ1234567890')
    code4 = choice('ABCDEFGHIJKLMNOPQRSTUVWYZ1234567890')
    code5 = choice('ABCDEFGHIJKLMNOPQRSTUVWYZ1234567890')
    code6 = choice('ABCDEFGHIJKLMNOPQRSTUVWYZ1234567890')
    return code1 + code2 + code3 + code4 + code5 + code6

def random_healthcard():
    hcnum = str(randint(1000000000, 9999999999))
    hcser1 = choice('ABCEGHJKLMNOPRSTVWXYZ')
    hcser2 = choice('ABCEGHJKLMNOPRSTVWXYZ')
    return  hcnum + hcser1 + hcser2
    
def random_answer1():
    return choice('YN')

def random_answer2():
    ans_list = ['ltc', 'rpa', 'others']
    return choice(ans_list)

def random_answer3():
    ans_list = ['diabetic', 'pregnant', 'cancer', 'none']
    return choice(ans_list)

def random_answer4():
    ans_list = ['doctor', 'nurse', 'ems', 'others']
    return choice(ans_list)

def random_domain():
    ans_list = ['@gmail.com', '@yahoo.com', '@yahoo.ca', '@loblaw.ca', '@google.com', '@microsoft.com', '@outlook.com']
    return choice(ans_list)

def gen_patient_id():
    patient_id = uuid.uuid4().hex
    return str(patient_id)

def gen_patient():
    
    patient_json = json.dumps({
    "patient": {
        "guid": "d290f1ee-6c54-4b01-90e6-d701748f0851",
        "regCode": "<tokenized>",
        "version": "0.1",
        "analogueRegistration": True,
        "givenName": "Luke",
        "middleName": "James",
        "familyName": "Skywalker",
        "yearMonthOfBirth": "195001 YYYYMM",
        "dateOfBirth": "<tokenized>",
        "gender": "male",
        "preferredLanguage": "en",
        "salutation": "mr",
        "enrollmentChannel": "mobile",
        "enrollmentBanner": "sdm",
        "enrollmentService": "formHero",
        "createTimestamp": "20210315T184247-0500 - (EST) Toronto",
        "updateTimestamp": "20210315T184247-0500 - (EST) Toronto",
        "patientData": {
            "schema": "/gs/tranche-subsystem/schema.js:v2",
            "resubmission": True,
            "marketingOptIn": True,
            "healthCardNumber": "<tokenized>",
            "healthCardIssuer": "ON",
            "screeningQuestions": {
                "Q1": "value",
                "Q2": "value",
                "Q3": "value",
                "Q4": "value",
                "Q5": "value"
                },
            "key": "value",
            "key1": "value",
            "key2": "value",
            "key3": "value",
            "key4": "value"
            },
        "emailAddress": "<tokenized>",
        "phoneNumber": "<tokenized>",
        "patientCommunicationData": {
            "phoneType": "mobile",
            "emailVerified": True,
            "key": "value",
            "key1": "value",
            "key2": "value"
            },
        "postalCode": "<tokenized>",
        "latitude": "50.1",
        "longitude": "50.1",
        "patientAddressData": {
            "addressVerified": True,
            "streetNumber": "<tokenized>",
            "unitNumber": "<tokenized>",
            "streetName": "<tokenized>",
            "city": "Toronto",
            "province": "Ontario",
            "provinceCode": "ON",
            "postalCode": "<tokenized>",
            "latitude": "50.1",
            "longitude": "50.2",
            "key1": "value",
            "key2": "value"
            }
        },
    "identity": [{
        "identityId": "d290f1ee-6c54-4b01-90e6-d701748f0851",
        "identityType": "PID",
        "identityValue": "4b01-90e6-d701748f0851",
        "identityFriendlyName": "Registration Code",
        "identityStatus": "ACTIVE",
        "identityCreateDate": "20210315T184247-0500 - (EST) Toronto",
        "identityUpdateDate": "20210315T184247-0500 - (EST) Toronto",
        "identityData": {}
        }],
    "journey": [{
        "journeyId": "d290f1ee-6c54-4b01-90e6-d701748f0851",
        "journeyType": "COVID-19",
        "journeyState": {
            "stateId": 1,
            "stateType": "REGISTERED",
            "stateStatus": "ACTIVE",
            "stateCreateDate": "20210315T184247-0500 - (EST) Toronto",
            "stateUpdateDate": "20210315T184247-0500 - (EST) Toronto"
            },
        "journeyCreateDate": "20210315T184247-0500 - (EST) Toronto",
        "journeyUpdateDate": "20210315T184247-0500 - (EST) Toronto",
        "journeyData": {},
        "tranche": {
            "trancheId": "d290f1ee-6c54-4b01-90e6-d701748f0851",
            "vaccine": {
                "vaccineId": 1,
                "vaccineType": "Pfizer-BioNTech",
                "injectionTotalCount": 2,
                "injectionMinTimeBetween": 2,
                "injectionNominalTimeBetween": 2,
                "injectionMaxTimeBetween": 7,
                "gtin-14": 7
                },
            "storeLocation": "SDM1234",
            "storeId": "SDM1234",
            "trancheCreateDate": "20210315T184247-0500 - (EST) Toronto",
            "trancheData": {}
            },
        "immunization": {
            "immunizationId": "d290f1ee-6c54-4b01-90e6-d701748f0851",
            "vaccine": {
                "vaccineId": 1,
                "vaccineType": "Pfizer-BioNTech",
                "injectionTotalCount": 2,
                "injectionMinTimeBetween": 2,
                "injectionNominalTimeBetween": 2,
                "injectionMaxTimeBetween": 7,
                "gtin-14": 7
                },
            "immunizationShotCurrent": 1,
            "immunizationShotTotal": 2,
            "vaccineAdministration": [{
                "vaccineAdministrationId": "d290f1ee-6c54-4b01-90e6-d701748f0851",
                "vaccineAdministrationSite": "Left-Arm",
                "vaccineAdministrationShotNumber": 1,
                "vaccineDosage": "0.3 mL",
                "vaccineBatch": "A09B12",
                "vaccineLot": "R0283",
                "vaccineExpiryDate": "2021-06-12",
                "vaccineAdministrationCreateDate": "20210315T184247-0500 - (EST) Toronto",
                "vaccineAdministrationUpdateDate": "20210315T184247-0500 - (EST) Toronto",
                "vaccineAdministrationData": {}
                }],
            "immunizationState": "INITIATED",
            "immunizationCreateDate": "20210315T184247-0500 - (EST) Toronto",
            "immunizationUpdateDate": "20210315T184247-0500 - (EST) Toronto"
            },
        "magicLinkies": [{
            "magicLinkyId": "d290f1ee-6c54-4b01-90e6-d701748f0851",
            "magicLinkyState": "ACTIVE",
            "magicLinkyURL": "https://url.ca",
            "magicLinkyCount": 1,
            "magicLinkyTotal": 3,
            "magicLinkyExpiryDate": "20210315T184247-0500 - (EST) Toronto",
            "magicLinkyCreateDate": "20210315T184247-0500 - (EST) Toronto",
            "magicLinkyUpdateDate": "20210315T184247-0500 - (EST) Toronto",
            "magicLinkyData": {}
            }],
        "notifications": [{
            "notificationId": "d290f1ee-6c54-4b01-90e6-d701748f0851",
            "notificationType": "STATE_CHANGE",
            "notificationChannel": "sdm",
            "notificationCreateDate": "20210315T184247-0500 - (EST) Toronto",
            "notificationUpdateDate": "20210315T184247-0500 - (EST) Toronto",
            "notificationData": {}
            }],
        "appointments": [{
            "appointmentId": "d290f1ee-6c54-4b01-90e6-d701748f0851",
            "appointmentCurrentState": "ACCEPTED",
            "appointmentTimestamp": "20210315T184247-0500 - (EST) Toronto",
            "appointmentLocation": "SDM1234",
            "cancellationLink": "https://cancelationlink.ca",
            "appointmentCreateDate": "20210315T184247-0500 - (EST) Toronto",
            "appointmentUpdateDate": "20210315T184247-0500 - (EST) Toronto",
            "appointmentData": {}
            }]
        }
        ]
    },indent=4)
    
    patient_dict = json.loads(patient_json)
    patient_id = gen_patient_id()
    regcode = random_regcode()
    dob = str(random_dob())
    hcnumber = random_healthcard()
    fullname = random_name()
    postalcode = random_postal()
    phonenum = random_phone()
    email = fullname[0].lower() + random_domain()

    patient_dict['patient']['guid'] = patient_id
    patient_dict['patient']['regCode'] = regcode
    patient_dict['patient']['dateOfBirth'] = dob
    patient_dict['patient']['patientData']['healthCardNumber'] = hcnumber
    patient_dict['patient']['emailAddress'] = email
    patient_dict['patient']['phoneNumber'] = phonenum
    patient_dict['patient']['postalCode'] = postalcode
    patient_dict['patient']['givenName'] = fullname[0]
    patient_dict['patient']['familyName'] = fullname[1]
    patient_dict['patient']['patientAddressData']['streetNumber'] = '10'
    patient_dict['patient']['patientAddressData']['unitNumber'] = '100'
    patient_dict['patient']['patientAddressData']['streetName'] = fullname[1] + ' ' + fullname[0]
    patient_dict['patient']['patientAddressData']['postalCode'] = postalcode

    # Tokenize
    plain_data = {
                "header":[
                    "regCode",
                    "dateOfBirth",
                    "healthCardNumber",
                    "emailAddress",
                    "phoneNumber",
                    "postalCode",
                    "streetNumber",
                    "unitNumber",
                    "streetName",
                    "postalCode2"
                ],
                "rows":[
                    [
                    patient_dict['patient']['regCode'],
                    patient_dict['patient']['dateOfBirth'],
                    patient_dict['patient']['patientData']['healthCardNumber'],
                    patient_dict['patient']['emailAddress'],
                    patient_dict['patient']['phoneNumber'],
                    patient_dict['patient']['postalCode'],
                    patient_dict['patient']['patientAddressData']['streetNumber'],
                    patient_dict['patient']['patientAddressData']['unitNumber'],
                    patient_dict['patient']['patientAddressData']['streetName'],
                    patient_dict['patient']['patientAddressData']['postalCode']
                    ]
                ]
            }

    enc_patient = dlp_tokenize(plain_data)    # returning a list

    patient_dict['patient']['regCode'] = str(enc_patient[0].string_value)
    patient_dict['patient']['dateOfBirth'] = str(enc_patient[1].string_value)
    patient_dict['patient']['patientData']['healthCardNumber'] = str(enc_patient[2].string_value)
    patient_dict['patient']['emailAddress'] = str(enc_patient[3].string_value)
    patient_dict['patient']['phoneNumber'] = str(enc_patient[4].string_value)
    patient_dict['patient']['postalCode'] = str(enc_patient[5].string_value)
    patient_dict['patient']['patientAddressData']['streetNumber'] = str(enc_patient[6].string_value)
    patient_dict['patient']['patientAddressData']['unitNumber'] = str(enc_patient[7].string_value)
    patient_dict['patient']['patientAddressData']['streetName'] = str(enc_patient[8].string_value)
    patient_dict['patient']['patientAddressData']['postalCode'] = str(enc_patient[9].string_value)

    screeningquestions_dict = {'Q1':random_answer1(),'Q2':random_answer1(),'Q3':random_answer4(),'Q4':random_answer2(),'Q5':random_answer3()}
    patient_dict['patient']['patientData']['screeningQuestions'] = screeningquestions_dict

    # Serializing json
    #patient_json = json.dumps(patient_dict, indent = 4)
    return patient_dict

def dlp_tokenize(data_dict):
    headers = [{"name": val} for val in data_dict["header"]]
    rows = []

    for row in data_dict["rows"]:
        rows.append( {"values": [{"string_value": cell_val} for cell_val in row]} )

    table = {}
    table["headers"] = headers
    table["rows"] = rows
    item = {"table": table}
    
    dlp = google.cloud.dlp.DlpServiceClient()
    parent = dlp.project_path(dlp_project_id)
    deidentify_template=f"projects/{dlp_project_id}/deidentifyTemplates/{dlp_template_id}"

    response = dlp.deidentify_content(parent, deidentify_template_name=deidentify_template,item=item)
    return response.item.table.rows[0].values # returning a list
  

def publish_patient(request):
    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(project_id, topic_id)

    request_json = request.get_json(silent=True)
    request_args = request.args
    
    #generate patients based on the number of patient argument
    if request_args and 'num_patients' in request_args:
        num_patients = int(request_args['num_patients'])
    else: 
        num_patients = 1
        
    for i in range(num_patients):
        #patient_dict = gen_patient()
        data_json =  json.dumps(gen_patient())
        data = data_json.encode("utf-8")

        try:
            future = publisher.publish(topic_path, data)
            future.result()
        except Exception as e:
            #print(e)
            return json.dumps({ 'Result':500, "errmsg":e })
    return json.dumps({ 'Result':'OK', 'NewPatients': num_patients })


#requirements.txt
#google-cloud-pubsub
#names
#google-cloud-dlp==1.0.0

#set GOOGLE_APPLICATION_CREDENTIALS=C:\python_codes\lt-aps-sbx-6a1381ce2963.json
#= 'C:\python_codes\access\lt-aps-sbx-dlp-6093342f78be.json'