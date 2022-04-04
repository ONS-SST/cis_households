_times_in_last_7_day_categories = {
    "None": 0,
    "1": 1,
    "2": 2,
    "3": 3,
    "4": 4,
    "5": 5,
    "6": 6,
    "7 times or more": 7,
}

_yes_no_categories = {"No": 0, "Yes": 1}

category_maps = {
    "iqvia_raw_category_map": {
        "agreed_to_additional_consent_visit": _yes_no_categories,
        "consent_blood_samples_if_positiveyn": _yes_no_categories,
        "consent_contact_extra_researchyn": _yes_no_categories,
        "consent_finger_prick_a1_a3": _yes_no_categories,
        "consent_blood_samples_if_positiveyn": _yes_no_categories,
        "consent_extend_study_under_16_b1_b3": _yes_no_categories,
        "consent_fingerprick_blood_samples": {"false": 0, "true": 1},
        "reconsented_blood": {"false": 0, "true": 1},
        "consent_extend_study_under_16_b1_b3": _yes_no_categories,
        "consent_use_of_surplus_blood_samplesyn": _yes_no_categories,
        "consent_blood_samples_if_positive": {"false": 0, "true": 1},
        "swab_taken": _yes_no_categories,
        "blood_taken": _yes_no_categories,
        "illness_lasting_over_12_months": _yes_no_categories,
        "have_ever_smoked_regularly": _yes_no_categories,
        "smoke_cigarettes": _yes_no_categories,
        "smokes_cigar": _yes_no_categories,
        "smokes_pipe": _yes_no_categories,
        "smokes_vape_e_cigarettes": _yes_no_categories,
        "smokes_hookah_shisha_pipes": _yes_no_categories,
        "smokes_nothing_now": _yes_no_categories,
        "work_nursing_or_residential_care_home": _yes_no_categories,
        "work_direct_contact_patients_clients": _yes_no_categories,
        "symptoms_last_7_days_any": _yes_no_categories,
        "symptoms_last_7_days_fever": _yes_no_categories,
        "symptoms_last_7_days_muscle_ache_myalgia": _yes_no_categories,
        "symptoms_last_7_days_fatigue_weakness": _yes_no_categories,
        "symptoms_last_7_days_sore_throat": _yes_no_categories,
        "symptoms_last_7_days_cough": _yes_no_categories,
        "symptoms_last_7_days_shortness_of_breath": _yes_no_categories,
        "symptoms_last_7_days_headache": _yes_no_categories,
        "symptoms_last_7_days_nausea_vomiting": _yes_no_categories,
        "symptoms_last_7_days_abdominal_pain": _yes_no_categories,
        "symptoms_last_7_days_diarrhoea": _yes_no_categories,
        "symptoms_last_7_days_loss_of_taste": _yes_no_categories,
        "symptoms_last_7_days_loss_of_smell": _yes_no_categories,
        "symptoms_last_7_days_more_trouble_sleeping": _yes_no_categories,
        "symptoms_last_7_days_runny_nose_sneezing": _yes_no_categories,
        "symptoms_last_7_days_noisy_breathing_wheezing": _yes_no_categories,
        "symptoms_last_7_days_loss_of_appetite": _yes_no_categories,
        "symptoms_last_7_days_chest_pain": _yes_no_categories,
        "symptoms_last_7_days_palpitations": _yes_no_categories,
        "symptoms_last_7_days_vertigo_dizziness": _yes_no_categories,
        "symptoms_last_7_days_worry_anxiety": _yes_no_categories,
        "symptoms_last_7_days_low_mood_not_enjoying_anything": _yes_no_categories,
        "symptoms_last_7_days_memory_loss_or_confusion": _yes_no_categories,
        "symptoms_last_7_days_difficulty_concentrating": _yes_no_categories,
        "think_have_covid_symptoms": _yes_no_categories,
        "is_self_isolating": _yes_no_categories,
        "received_shielding_letter": _yes_no_categories,
        "contact_known_positive_covid_last_28_days": _yes_no_categories,
        "contact_suspected_positive_covid_last_28_days": _yes_no_categories,
        "hospital_last_28_days": _yes_no_categories,
        "hospital_last_28_days_other_household_member": _yes_no_categories,
        "care_home_last_28_days": _yes_no_categories,
        "care_home_last_28_days_other_household_member": _yes_no_categories,
        "think_had_covid": _yes_no_categories,
        "think_had_covid_contacted_nhs": _yes_no_categories,
        "think_had_covid_admitted_to_hospital": _yes_no_categories,
        "think_had_covid_any_symptoms": _yes_no_categories,
        "symptoms_since_last_visit_fever": _yes_no_categories,
        "symptoms_since_last_visit_muscle_ache_myalgia": _yes_no_categories,
        "symptoms_since_last_visit_fatigue_weakness": _yes_no_categories,
        "symptoms_since_last_visit_sore_throat": _yes_no_categories,
        "symptoms_since_last_visit_cough": _yes_no_categories,
        "symptoms_since_last_visit_shortness_of_breath": _yes_no_categories,
        "symptoms_since_last_visit_headache": _yes_no_categories,
        "symptoms_since_last_visit_nausea_vomiting": _yes_no_categories,
        "symptoms_since_last_visit_abdominal_pain": _yes_no_categories,
        "symptoms_since_last_visit_diarrhoea": _yes_no_categories,
        "symptoms_since_last_visit_loss_of_taste": _yes_no_categories,
        "symptoms_since_last_visit_loss_of_smell": _yes_no_categories,
        "symptoms_since_last_visit_more_trouble_sleeping": _yes_no_categories,
        "symptoms_since_last_visit_runny_nose_sneezing": _yes_no_categories,
        "symptoms_since_last_visit_noisy_breathing_wheezing": _yes_no_categories,
        "symptoms_since_last_visit_loss_of_appetite": _yes_no_categories,
        "symptoms_since_last_visit_chest_pain": _yes_no_categories,
        "symptoms_since_last_visit_palpitations": _yes_no_categories,
        "symptoms_since_last_visit_vertigo_dizziness": _yes_no_categories,
        "symptoms_since_last_visit_worry_anxiety": _yes_no_categories,
        "symptoms_since_last_visit_low_mood_or_not_enjoying_anything": _yes_no_categories,
        "symptoms_since_last_visit_memory_loss_or_confusion": _yes_no_categories,
        "symptoms_since_last_visit_difficulty_concentrating": _yes_no_categories,
        "symptoms_since_last_visit_low_mood_or_not_enjoying_anything": _yes_no_categories,
        "other_pcr_test_since_last_visit": _yes_no_categories,
        "other_antibody_test_since_last_visit": _yes_no_categories,
        "been_outside_uk_since_last_visit": _yes_no_categories,
        "have_long_covid_symptoms": _yes_no_categories,
        "long_covid_fever": _yes_no_categories,
        "long_covid_weakness_tiredness": _yes_no_categories,
        "long_covid_diarrhoea": _yes_no_categories,
        "long_covid_loss_of_smell": _yes_no_categories,
        "long_covid_shortness_of_breath": _yes_no_categories,
        "long_covid_vertigo_dizziness": _yes_no_categories,
        "long_covid_trouble_sleeping": _yes_no_categories,
        "long_covid_headache": _yes_no_categories,
        "long_covid_nausea_vomiting": _yes_no_categories,
        "long_covid_loss_of_appetite": _yes_no_categories,
        "long_covid_sore_throat": _yes_no_categories,
        "long_covid_chest_pain": _yes_no_categories,
        "long_covid_worry_anxiety": _yes_no_categories,
        "long_covid_memory_loss_or_confusion": _yes_no_categories,
        "long_covid_muscle_ache": _yes_no_categories,
        "long_covid_abdominal_pain": _yes_no_categories,
        "long_covid_loss_of_taste": _yes_no_categories,
        "long_covid_cough": _yes_no_categories,
        "long_covid_palpitations": _yes_no_categories,
        "long_covid_low_mood": _yes_no_categories,
        "long_covid_difficulty_concentrating": _yes_no_categories,
        "long_covid_runny_nose_sneezing": _yes_no_categories,
        "long_covid_noisy_breathing": _yes_no_categories,
        "confirm_received_vouchers": _yes_no_categories,
        "have_landline_number": _yes_no_categories,
        "have_mobile_number": _yes_no_categories,
        "have_email_address": _yes_no_categories,
        "work_main_job_changed": _yes_no_categories,
        "work_in_additional_paid_employment": _yes_no_categories,
        "cis_covid_vaccine_received": _yes_no_categories,
        "cis_flu_vaccine_received": _yes_no_categories,
        "did_not_attend_inferred": _yes_no_categories,
        "visit_order": {
            "First Visit": 1,
            "Follow-up 1": 2,
            "Follow-up 2": 3,
            "Follow-up 3": 4,
            "Follow-up 4": 5,
            "Month 2": 6,
            "Month 3": 7,
            "Month 4": 8,
            "Month 5": 9,
            "Month 6": 10,
            "Month 7": 11,
            "Month 8": 12,
            "Month 9": 13,
            "Month 10": 14,
            "Month 11": 15,
            "Month 12": 16,
            "Month 13": 17,
            "Month 14": 18,
            "Month 15": 19,
            "Month 16": 20,
            "Month 17": 21,
            "Month 18": 22,
            "Month 19": 23,
            "Month 20": 24,
            "Month 21": 25,
            "Month 22": 26,
            "Month 23": 27,
            "Month 24": 28,
        },
        "participant_visit_status": {
            "Cancelled": 0,
            "Completed": 1,
            "Patient did not attend": 2,
            "Participant did not attend": 2,
            "Re-scheduled": 3,
            "Scheduled": 4,
            "Partially Completed": 5,
            "Withdrawn": 6,
            "New": 7,
            "Dispatched": 8,
            "Household did not attend": 9,
        },
        "visit_type": {"First Visit": 0, "Follow-up Visit": 1},
        "sex": {"Male": 1, "Female": 2},
        "ethnicity": {
            "White-British": 1,
            "White-Irish": 2,
            "White-Gypsy or Irish Traveller": 3,
            "Any other white background": 4,
            "Mixed-White & Black Caribbean": 5,
            "Mixed-White & Black African": 6,
            "Mixed-White & Asian": 7,
            "Any other Mixed background": 8,
            "Asian or Asian British-Indian": 9,
            "Asian or Asian British-Pakistani": 10,
            "Asian or Asian British-Bangladeshi": 11,
            "Asian or Asian British-Chinese": 12,
            "Any other Asian background": 13,
            "Black,Caribbean,African-African": 14,
            "Black,Caribbean,Afro-Caribbean": 15,
            "Any other Black background": 16,
            "Other ethnic group-Arab": 17,
            "Any other ethnic group": 18,
        },
        "illness_reduces_activity_or_ability": {"Not at all": 0, "Yes, a little": 1, "Yes, a lot": 2},
        "work_sectors": {
            "Teaching and education": 1,
            "Health care": 2,
            "Social care": 3,
            "Transport (incl. storage, logistic)": 4,
            "Retail sector (incl. wholesale)": 5,
            "Hospitality (e.g. hotel, restaurant)": 6,
            "Food production, agriculture, farming": 7,
            "Personal services (e.g. hairdressers)": 8,
            "Information technology and communication": 9,
            "Financial services incl. insurance": 10,
            "Manufacturing or construction": 11,
            "Civil service or Local Government": 12,
            "Armed forces": 13,
            "Arts,Entertainment or Recreation": 14,
            "Other occupation sector": 15,
            "NA(Not currently working)": 99,
        },
        "work_health_care_v1_v2_raw": {
            "No": 0,
            "Yes, in primary care, e.g. GP, dentist": 1,
            "Yes, in secondary care, e.g. hospital": 2,
            "Yes, in other healthcare settings, e.g. mental health": 3,
        },
        "work_health_care_combined": {
            "No": 0,
            "Yes, primary care, patient-facing": 1,
            "Yes, secondary care, patient-facing": 2,
            "Yes, other healthcare, patient-facing": 3,
            "Yes, primary care, non-patient-facing": 4,
            "Yes, secondary care, non-patient-facing": 5,
            "Yes, other healthcare, non-patient-facing": 6,
        },
        "work_social_care": {
            "No": 0,
            "Yes, care/residential home, resident-facing": 1,
            "Yes, other social care, resident-facing": 2,
            "Yes, care/residential home, non-resident-facing": 3,
            "Yes, other social care, non-resident-facing": 4,
        },
        "work_status_v0": {
            "Employed": 1,
            "Self-employed": 2,
            "Furloughed (temporarily not working)": 3,
            "Not working (unemployed, retired, long-term sick etc.)": 4,
            "Student": 5,
        },
        "work_status_v1": {
            "Employed and currently working": 1,
            "Employed and currently not working": 2,
            "Self-employed and currently working": 3,
            "Self-employed and currently not working": 4,
            "Looking for paid work and able to start": 5,
            "Not working and not looking for work": 6,
            "Retired": 7,
            "Child under 5y not attending child care": 8,
            "Child under 5y attending child care": 9,
            "5y and older in full-time education": 10,
        },
        "work_status_v2": {
            "Employed and currently working": 1,
            "Employed and currently not working": 2,
            "Self-employed and currently working": 3,
            "Self-employed and currently not working": 4,
            "Looking for paid work and able to start": 5,
            "Not working and not looking for work": 6,
            "Retired": 7,
            "Child under 4-5y not attending child care": 8,
            "Child under 4-5y attending child care": 9,
            "4-5y and older at school/home-school": 10,
            "Attending college or FE (including if temporarily absent)": 11,
            "Attending university (including if temporarily absent)": 12,
        },
        "work_location": {
            "Working from home": 1,
            "Working somewhere else (not your home)": 2,
            "Both (from home and somewhere else)": 3,
            "Not applicable, not currently working": 4,
        },
        "ability_to_socially_distance_at_work_or_school": {
            "Easy to maintain 2m": 1,
            "Relatively easy to maintain 2m": 2,
            "Difficult to maintain 2m, but can be 1m": 3,
            "Very difficult to be more than 1m away": 4,
            "N/A (not working/in education etc)": 9,
        },
        "transport_to_work_or_school": {
            "Underground, metro, light rail, tram": 1,
            "Train": 2,
            "Bus, minibus, coach": 3,
            "Motorbike, scooter or moped": 4,
            "Car or van": 5,
            "Taxi/minicab": 6,
            "Bicycle": 7,
            "On foot": 8,
            "Other method": 9,
            "N/A (not working/in education etc)": 99,
        },
        "is_self_isolating_detailed": {
            "No": 0,
            "Yes, you have/have had symptoms": 1,
            "Yes, someone you live with had symptoms": 2,
            "Yes, for other reasons (e.g. going into hospital, quarantining)": 3,
        },
        "last_covid_contact_location": {"Living in your own home": 1, "Outside your home": 2},
        "last_suspected_covid_contact_location": {"Living in your own home": 1, "Outside your home": 2},
        "household_been_hospital_last_28_days": {
            "No, no one in my household has": 0,
            "Yes, I have": 1,
            "No I haven’t, but someone else in my household has": 2,
        },
        "household_been_care_home_last_28_days": {
            "No, no one in my household has": 0,
            "Yes, I have": 1,
            "No I haven’t, but someone else in my household has": 2,
        },
        "physical_contact_under_18_years": {"0": 0, "1-5": 1, "6-10": 2, "11-20": 3, "21 or more": 4},
        "physical_contact_18_to_69_years": {"0": 0, "1-5": 1, "6-10": 2, "11-20": 3, "21 or more": 4},
        "physical_contact_over_70_years": {"0": 0, "1-5": 1, "6-10": 2, "11-20": 3, "21 or more": 4},
        "social_distance_contact_under_18_years": {"0": 0, "1-5": 1, "6-10": 2, "11-20": 3, "21 or more": 4},
        "social_distance_contact_18_to_69_years": {"0": 0, "1-5": 1, "6-10": 2, "11-20": 3, "21 or more": 4},
        "social_distance_contact_over_70_years": {"0": 0, "1-5": 1, "6-10": 2, "11-20": 3, "21 or more": 4},
        "face_covering_outside_of_home": {
            "No": 0,
            "Yes, at work/school only": 1,
            "Yes, in other situations only": 2,
            "Yes, usually both Work/school/other": 3,
            "My face is already covered": 4,
        },
        "face_covering_work": {
            "Never": 0,
            "Yes, sometimes": 1,
            "Yes, always": 2,
            "Not going to place of work or education": 3,
            "My face is already covered": 4,
            "My face is already covered for other reasons such as religious or cultural reasons": 5,
        },
        "face_covering_other_enclosed_places": {
            "Never": 0,
            "Yes, sometimes": 1,
            "Yes, always": 2,
            "Not going to other enclosed public spaces or using public transport": 3,
            "My face is already covered": 4,
            "My face is already covered for other reasons such as religious or cultural reasons": 5,
        },
        "other_pcr_test_results": {
            "Any tests negative, but none positive": 0,
            "One or more positive test(s)": 1,
            "Waiting for all results": 2,
            "All Tests failed": 9,
        },
        "other_antibody_test_results": {
            "Any tests negative, but none positive": 0,
            "One or more positive test(s)": 1,
            "Waiting for all results": 2,
            "All Tests failed": 9,
        },
        "other_antibody_test_location": {"In the NHS (e.g. GP, hospital)": 1, "Private lab": 2, "Home test": 3},
        "long_covid_reduce_activities": {"Not at all": 4, "Yes a little": 5, "Yes a lot": 6},
        "withdrawal_type": {
            "Withdrawn": 1,
            "Withdrawn_no_future_linkage_or_use of samples": 2,
            "Withdrawn_no_future_linkage": 3,
        },
        "country_barcode": {"England": 0, "Wales": 1, "NI": 2, "Scotland": 3},
        "not_attended_reason": {
            "At School": 1,
            "At Work": 2,
            "Doctor Appointment": 3,
            "Living Away - For Education": 4,
            "On Holiday": 5,
            "Other": 6,
            "Phone not answered": 7,
            "Planning to withdraw": 8,
        },
        "deferred": {"NA": 0, "Deferred": 1},
        "times_hour_or_longer_another_home_last_7_days": _times_in_last_7_day_categories,
        "times_hour_or_longer_another_person_your_home_last_7_days": _times_in_last_7_day_categories,
        "prefer_receive_vouchers": {"Email": 1, "Paper": 2},
        "participant_testing_group": {"Swab Only": 0, "Blood and Swab": 1, "Fingerprick and Swab": 2},
        "household_fingerprick_status": {
            "Accepted": 0,
            "Declined": 1,
            "Invited": 2,
            "At least one person consented": 3,
            "Not invited": 4,
            "No-one Consented": 5,
        },
        "able_to_take_blood": _yes_no_categories,
        "no_fingerprick_blood_taken_reason": {
            "Bruising or pain after first attempt": 0,
            "Couldn't get enough blood": 1,
            "No stock": 2,
            "Other": 3,
            "Participant felt unwell/fainted": 4,
            "Participant refused to give blood on this visit": 5,
            "Participant time constraints": 6,
            "Two attempts made": 7,
            "High risk assessment outcome": 8,
        },
        "no_venous_blood_taken_reason": {
            "Non-contact visit. Household self-isolating": 0,
            "Participant dehydrated": 1,
            "No stock": 2,
            "Other": 3,
            "Participant felt unwell/fainted": 4,
            "Participant refused": 5,
            "Participant time constraints": 6,
            "Poor venous access": 7,
            "Two attempts made": 8,
            "Bruising or pain after first attempt": 9,
        },
        "accepted_fingerprick_invite": _yes_no_categories,
        "cis_covid_vaccine_offered": _yes_no_categories,
        "is_regularly_lateral_flow_testing": _yes_no_categories,
        "household_visit_status": {
            "Completed": 1,
            "Dispatched": 2,
            "Household did not attend": 3,
            "Partially Completed": 4,
            "Withdrawn": 5,
        },
        "participant_survey_status": {"Active": 0, "Withdrawn": 1, "Completed": 2},
        "withdrawal_reason": {
            "Bad experience with interviewer/survey": 1,
            "Moving location": 2,
            "No longer convenient": 3,
            "No longer wants to take part": 4,
            "Participant does not want to self swab": 5,
            "Swab/blood process too distressing": 6,
            "Too many visits": 7,
            "Household declined": 8,
            "Deceased": 9,
            "Do not reinstate": 10,
            "SWCAP": 11,
        },
        "cis_covid_vaccine_type": {
            "Don't know type": 1,
            "From a research study/trial": 2,
            "Moderna": 3,
            "Oxford/AstraZeneca": 4,
            "Pfizer/BioNTech": 5,
            "Other / specify": 6,
            "Janssen/Johnson&Johnson": 7,
            "Novavax": 8,
            "Sinovac": 9,
            "Sinovax": 10,
            "Valneva": 11,
            "Sinopharm": 12,
            "Sputnik": 13,
        },
        "cis_covid_vaccine_type_1": {
            "Don't know type": 1,
            "From a research study/trial": 2,
            "Moderna": 3,
            "Oxford/AstraZeneca": 4,
            "Pfizer/BioNTech": 5,
            "Other / specify": 6,
            "Janssen/Johnson&Johnson": 7,
            "Novavax": 8,
            "Sinovac": 9,
            "Sinovax": 10,
            "Valneva": 11,
            "Sinopharm": 12,
            "Sputnik": 13,
        },
        "cis_covid_vaccine_type_2": {
            "Don't know type": 1,
            "From a research study/trial": 2,
            "Moderna": 3,
            "Oxford/AstraZeneca": 4,
            "Pfizer/BioNTech": 5,
            "Other / specify": 6,
            "Janssen/Johnson&Johnson": 7,
            "Novavax": 8,
            "Sinovac": 9,
            "Sinovax": 10,
            "Valneva": 11,
            "Sinopharm": 12,
            "Sputnik": 13,
        },
        "cis_covid_vaccine_type_3": {
            "Don't know type": 1,
            "From a research study/trial": 2,
            "Moderna": 3,
            "Oxford/AstraZeneca": 4,
            "Pfizer/BioNTech": 5,
            "Other / specify": 6,
            "Janssen/Johnson&Johnson": 7,
            "Novavax": 8,
            "Sinovac": 9,
            "Sinovax": 10,
            "Valneva": 11,
            "Sinopharm": 12,
            "Sputnik": 13,
        },
        "cis_covid_vaccine_type_4": {
            "Don't know type": 1,
            "From a research study/trial": 2,
            "Moderna": 3,
            "Oxford/AstraZeneca": 4,
            "Pfizer/BioNTech": 5,
            "Other / specify": 6,
            "Janssen/Johnson&Johnson": 7,
            "Novavax": 8,
            "Sinovac": 9,
            "Sinovax": 10,
            "Valneva": 11,
            "Sinopharm": 12,
            "Sputnik": 13,
        },
        "cis_covid_vaccine_number_of_doses": {"1": 1, "2": 2, "3 or more": 3},
        "visit_date_type": {
            "actual_visit_date": 0,
            "latest_checkin_date": 1,
            "sample_taken_date": 2,
            "scheduled_date": 3,
        },
    },
}
