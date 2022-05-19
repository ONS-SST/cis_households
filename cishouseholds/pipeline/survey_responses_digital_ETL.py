from cishouseholds.derive import derive_self_isolating_from_digital

# from cishouseholds.derive import derive_had_symptom_last_7days_from_digital


def digital_specific_cleaning(df):
    df = derive_self_isolating_from_digital(df, "self_isolating_detailed", "self_isolating", "self_isolating_reason")
    # df = derive_had_symptom_last_7days_from_digital(df, "had_symptom_last_7days", "think_have_covid_symptom_.{1,}")
    # df = derive_had_symptom_last_7days_from_digital(
    #     df,
    #     "had_symptom_last_7days",
    #     "think_have_covid_symptom",
    #     [
    #         "fever"
    #         "muscle_ache_myalgia"
    #         "fatigue_weakness"
    #         "sore_throat"
    #         "cough"
    #         "shortness_of_breath"
    #         "headache"
    #         "nausea_vomiting"
    #         "abdominal_pain"
    #         "diarrhoea"
    #         "loss_of_taste"
    #         "loff_of_smell"
    #     ],
    # )
    return df