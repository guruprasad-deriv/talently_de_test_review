# Interview Prompts Log

All prompts are spell-corrected and saved in order.

---

## Section 1: Context & Setup

### Prompt 1

I am giving a technical interview for a company where I have been asked to use AI for code submissions and screen is also being recorded. I will share contents — try to observe all these data and we can solve challenges one by one. Finally we need to submit this output to GitHub in a format and also I have been asked to save all my user prompts for submission as well, so please start creating memory and prompts file and store all of my prompts starting from this (correct spell checks during prompt saving). Once we are ready we can start and save all files in a new folder and all data files under that new folder itself.

### Prompt 2

Also, I am giving a test for tech and team lead level data engineering for a trading company. Also, key here is to reason out with me before committing to a challenge.

### Prompt 3

I just want to check — is my previous prompt saved?

### Prompt 4

Can we categorize these prompts into different sections? So all initial ones are context collection, and later when we jump on to challenges we can go ahead and have a different section for it.

---

## Section 2: Challenges

### Prompt 5

[Full challenge brief pasted — BUILD assessment for production-grade data engineering solution for a financial trading platform. Contains: 4 JSON core tables (client_signup, client_profile, client_deposit, client_trades), 3 vendor CSV files with intentional anomalies, 1 CDC JSONL file, 1 scale_profile.md. Four parts: Pipeline Design + DQ, Data Model + SCD, Scalability Diagnosis, Real-time Architecture + Build vs Buy. Deliverable: GitHub repo with specific structure including PROMPTS.md.]

---

### Prompt 6

Let's not get into data analysis first. Can we segregate all JSON contents into separate files and validate if we are not missing any records? I can do one manual validation and let you take care of the rest.

---

### Prompt 7

Yes, let's try to understand each table now. As we are discussing about pipeline design, let's understand how the data in client_signup, client_profile, client_deposit, and client_trades would have been loaded. Does it have any timestamp field which indicates when it was last loaded, or any sequence number etc?

---

### Prompt 8

Yeah, we are about to solve Part 1a and each section underneath. Before that, show me complete data once in tabular form for me to review.

---

### Prompt 9

No, I can't see the table. Let's go one by one on already existing tables in warehouse now.

---

### Prompt 10

Yes, save it. I will review this later. Let's move to client_profile.

---

### Prompt 11

Yes.

---

### Prompt 12

One thing to cover in the first two tables: is nationality or country field different or same? And similarly, do you want to compare any other field between client_signup and client_profile? Also for last_login_date null, see if client signup or KYC is pending. And also compare signup date vs last login date.

---

### Prompt 13

Save all these data quality issues into one table as and when you find them.

---

### Prompt 14

Yes.

---

### Prompt 15

I have this plan in mind — I do not want to mutate during runtime. I want to land the data as-is into the landing layer with an insert_timestamp. Since we can have schema drift, I would not store fields as-is; I would store the row as a JSON blob. So the landing layer would have separate tables for each different functionality — in this case vendor deposits and client profile — so 2 tables. Fields would be file_name, metadata_json, and insert_timestamp. The landing layer would have duplicates and I do not care about late-arriving records. The partition is on insert_timestamp. Once data lands in the landing layer, if the challenge is not asking about real-time use cases, I would be doing batch runs. Please validate this architecture against all use cases, challenges, and questions in this challenge.

---

### Prompt 16

For the file manifest — I would use a checksum on the whole file to see if the file changes. If the file changes, load the complete contents as-is. My dedup key in staging would be: delete all records with that file name and reload if checksum differs; if no checksum difference, ignore.

---

### Prompt 17

For question 4 (source-delete handling), I would do soft delete.

---

### Prompt 18

I see this "One Gap to Address" — the deposit_id format mismatch. Warehouse uses DEP###, vendor uses VDEP###. Is this data for reconciliation, or are they new inserts?

---

### Prompt 19

[Full challenge brief pasted — this is the complete BUILD assessment for a production-grade data engineering solution for a financial trading platform. Confirmed final file structure: README.md, part1_pipeline.md, part2_data_model.md, part3_diagnosis.md, part4_architecture.md, sql/, code/, PROMPTS.md. Also confirmed: runnable idempotent prototype in code/ is a strong positive signal. PROMPTS.md must list every AI prompt grouped by part with what was changed/decided from the output.]

---

### Prompt 20

In staging I would do inserts only but without error records. In the final model layer I would merge this data by data pruning in target — basically if records don't exist in target then insert, else ignore, considering there are no mutations in the data for this specifically.

---

### Prompt 21

[User pointed out that Query A sort order was wrong: "zero-deposit countries first, then remaining countries descending by deposit count" — the ORDER BY deposit_count ASC was sorting all non-zero countries ascending. Fix: use CASE WHEN COUNT = 0 THEN 0 ELSE 1 END ASC, then COUNT DESC, then country ASC. Fixed in both sql/query_a_deposit_count_by_country.sql and part2_data_model.md.]

---

### Prompt 22

Spin multiple agents to get the job done.

---

### Prompt 23

For Part 4 I will not buy a new tool for a single use case. The idea is to see what the future requirements are and if these tools provide batch data and if batch data is required I would build a custom integration with AI-generated code and run it on existing infra, as it does not make sense to onboard a tool for one use case. I would go for tools like Fivetran if we need complex API structure data to be onboarded and have multiple requirements in future.

---

### Prompt 24

Hope all prompts are spell-checked before pushing and no secrets to be pushed here — validate before push.

---
