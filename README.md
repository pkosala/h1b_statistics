# H1B Annual Report
This project generates reports to analyze the H1B Visa Applications. It measures two metrics namely: Top 10 Occupations and Top 10 States of VISA applicants. These reports are generated annually.

### Problem
```
Each year's data has different schema. This program should be able to identify the required fields to create 2 output files:

* top_10_occupations.txt: Top 10 occupations for certified visa applications
* top_10_states.txt: Top 10 states for certified visa applications
```
### Approach
```
* Storing the data: I created a list of dictionaries to store the data. This data structure handles variable schema better and scalable for large datasets.

* Generic function to filter, select and sort data: I created a generic function which can apply complex filters using lambda expressions. Since we do not need all the features present in the, this function projects the required fields.
    It also performs custom sort. For example, if you want to sort the data (A,B, C) as : Descending order for C, ascending A and descending on B. This generic function handles this as well.

* Top 10 Occupations:
    Firstly, identify the fields and combine them using the logic described in issue#1
    Filter for rows with certification status as "CERTIFIED" and select only required columns using the generic function described above.
    While filtering in above step, data is sorted by SOC_Name field so that all rows with similar soc_names are placed together, this helps while applying group by later
    Group the SOC_Name field and count the case_number, calculate the percentage in parallel.
    Sort the grouped output by number of applicants in descending and occupation name in ascending order.

* Top 10 States:
    Firstly, identify the fields and combine them using the logic described in issue#1
    Filter for rows with certification status as "CERTIFIED" and select only required columns using the generic function described above.
    While filtering in above step, data is sorted by Work_state field so that all rows with similar Work_states are placed together, this helps while applying group by later
    Group the Work_state field and count the case_number, calculate the percentage in parallel
    Sort the grouped output by number of applicants in descending and Work_state in ascending order.

* Issue #1:
    Verifying the data present in [UNITED STATES DEPARTMENT OF LABOR Employment & Training Administration](https://www.foreignlaborcert.doleta.gov/performancedata.cfm#dis), I realized that all the necessary fields have duplicate fields within the document.
    For example, state1 and state2 both are work state fields necessary for generating top 10 states metric.
    Here is the approach I used to handle such fields:
    ** Create a static list of possible field names to match across. Match the required column with its static list
    ** If you found atleast 1 match; use it, else apply regex to identify the field.
    ** If after above two operations, we still get more than 1 field for a required column, we merge the fields into one, using following logic:
        *** Assumption: It is assumed that the duplicate fields are stored in order of priority in the data file.
        *** The first field is used as base, subsequent field's values are overwritten on to the base field only if it is found empty or None.
        *** This way the first field has all values form all the fields it is represented by.
* Issue #2:
    Most of the fields have empty string ''. I am not sure if this should be projected in the output.
    ** In my assumption, we should not include such rows in calculating the metrics. So I excluded such rows. For example, an empty string for state or soc_name is not desirable.
*Issue #3:
    A special of issue #1 is for soc_code and soc_name, there are rows which have soc_code but not soc_name, I chose to ignore such rows.
* Issue #4:
    Assumption: Even a non-empty file generates empty output. In such cases, the program creates output files with just the headers.


```
### Prerequisites
```
* [Python 3.7](https://www.python.org/)
* Linux Shell
```

### Installation

Clone or download the repository. Go to the root folder and run the shell script using following command:

```
./run.sh
```

## Running the tests

Go to the /insight_testsuite folder and run the shell script using following command:
```
./run_tests.sh
```
```
=========================================================
=========================================================
=========================================================
==========EXPLAIN TEST CASES HERE==========================
=========================================================
=========================================================
=========================================================
```
## Authors

* **Pooja Kosala**

## Acknowledgments

* [Insight Data Engineering](https://www.insightdataengineering.com/)
* [Stack OverFlow](https://stackoverflow.com/)
* [UNITED STATES DEPARTMENT OF LABOR Employment & Training Administration](https://www.foreignlaborcert.doleta.gov/performancedata.cfm#dis)
