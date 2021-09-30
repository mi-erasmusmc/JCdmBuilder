2021-09-29 Combined the original primary_keys and indices scripts to a pk indices script.
2021-09-29 Replaced the type bigint of attribute person_id with integer in table EPISODE.
2021-09-30 Added primary key constraints:
              ALTER TABLE COHORT ADD CONSTRAINT xpk_COHORT PRIMARY KEY (cohort_definition_id, subject_id, cohort_start_date, cohort_end_date);
              ALTER TABLE COHORT_DEFINITION ADD CONSTRAINT xpk_COHORT_DEFINITION PRIMARY KEY (cohort_definition_id);
           Replaced foreign key constraint: ALTER TABLE COHORT_DEFINITION ADD CONSTRAINT fpk_COHORT_DEFINITION_cohort_definition_id FOREIGN KEY (cohort_definition_id) REFERENCES COHORT (COHORT_DEFINITION_ID);
                                      with: ALTER TABLE COHORT ADD CONSTRAINT fpk_COHORT_cohort_definition_id FOREIGN KEY (cohort_definition_id) REFERENCES COHORT_DEFINITION (COHORT_DEFINITION_ID);