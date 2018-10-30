import csv
import re
import sys
from operator import itemgetter
from functools import cmp_to_key
from itertools import groupby
import logging
logger = logging.getLogger(__name__)

# static list of possible column names
static_soc_name_field_list = ['pw_soc_title','pw_soc_name', 'suggested_soc_title','suggested_soc_name', 'pwd_soc_title', 'pwd_soc_name', 'soc_title', 'lca_case_soc_name','lca_case_soc_title','soc_name']
static_soc_code_field_list = ['pw_soc_code', 'suggested_soc_code', 'pwd_soc_code', 'soc_code', 'pw soc code', 'lca_case_soc_code']
static_certification_status_field_list = ['case_status', 'case status', 'approval_status', 'status']
static_case_number_field_list = ['case_number', 'case number', 'case_no', 'lca_case_number']
static_work_state_field_list = ['job_info_work_state', 'primary_worksite_state', 'worksite_state',
                                'job info work state', 'alien_work_state', 'worksite_location_state',
                                'state_2', 'state_1', 'lca_case_workloc1_state', 'lca_case_workloc2_state']

def load_data(filename):
    """Load data from a csv file

    Args:
      filename(str): filename to read
    Returns:
      (headers, data): Columns name of the data and the data
    Raises:
      ValueError: Unable to read file
    """
    data = []
    try:
        with open(filename, newline='', encoding="utf8") as fromfile:
            reader = csv.reader(fromfile, delimiter=';')
            headers = list(map(lambda x:x.lower(),next(reader, None)))
            rdr = csv.DictReader(fromfile, headers, delimiter=';', quotechar='"')
            data.extend(rdr)
    except IOError as err:
        logger.exception('Error occurred while reading the data from file: ' + filename)
        raise err
    if not data:
        raise ValueError('No data available')
    return headers, data

def write_data(headers, data, filename):
    """Write data to a csv file.

    Args:
        headers: Columns name of the data
        data (list): List of dictionaries
        filename(str): Filename to write to
    Returns:
        (headers, data): Columns name of the data and the data
    Raises:
        ValueError: Unable to write to file
                    No data to write error

    """
    try:
        keys = headers
        with open(filename, 'w', newline='', encoding="utf8") as tofile:
            dict_writer = csv.DictWriter(tofile, keys,delimiter=';')
            dict_writer.writeheader()
            dict_writer.writerows(data)
    except IOError as err:
        logger.exception('Error occurred while writing the data to file ')
        raise err


def query_load(data, filter_expression=None, project_keys=None, sort_keys=None, sort_order=None):
    """Generic function to filter, select fields and sort data

    Args:
        data (list): List of dictionaries
        filter_expression(lambda expression): Lambda expression for filtering the data
        project_keys(list): Columns to project
        sort_keys(list): Columns to sort on. This should be subset of project_keys when project_keys is passed
        sort_order(list): Sort order for columns passed as sort_keys. Length of sort_order should be similar to sort_keys;
                            Pass only 'Ascending', 'Descending'
    Returns:
        data (list): List of dictionaries
    Raises:
        ValueError: Error occurred while trying to filter, project or sort the data

    """
    try:
        if filter_expression is not None:
            data = [r for r in data if filter_expression(r)]

        if project_keys is not None:
            data = [{k: v for k, v in i.items() if k in project_keys} for i in data]

        if sort_keys is not None:
            if sort_order is not None:
                if len(sort_order) == len(sort_keys):
                    # Helper to custom sort the data
                    comparers = [((itemgetter(col.strip()), -1) if order == 'Descending' else
                                  (itemgetter(col.strip()), 1)) for col, order in zip(sort_keys, sort_order)]

                    def comparer(left, right):
                        comparer_iter = (
                            cmp(fn(left), fn(right)) * mult
                            for fn, mult in comparers
                        )
                        return next((result for result in comparer_iter if result), 0)

                    def cmp(a, b):
                        return (a > b) - (a < b)

                    return sorted(data, key=cmp_to_key(comparer))
                else:
                    raise ValueError('Length of sort order and sort keys is not same')
            else:
                data = sorted(data, key=lambda r: [r[k] for k in sort_keys])
        return data
    except Exception as err:
        logger.exception("Error occurred while trying to filter, project or sort the data")
        raise err

def percentage(part, whole):
    """Calculates the percentage of part of whole

    Args:
        part (integer): Part of the total whose percent is calculated
        whole (integer): Total value

    Returns:
        data (float): float value with precision of 1 digit after decimal
    Raises:
        ValueError: Error occurred while trying to filter, project or sort the data
    """
    try:
        if not whole:
            raise ValueError("Denominator cannot be 0")
        return round(float(100) * float(part) / float(whole), 1)
    except Exception as err:
        logger.exception("Error occurred while calculating percentage")
        raise err


def get_top_occupations(count, data, soc_name_field_list,soc_code_field_list, certification_status_field_list, case_number_field_list):
    """Top n occupations for certified visa applications

    Args:
        count (integer): Part of the total whose percent is calculated
        data (list): List of dictionaries
        soc_name_field_list: List of field names which holds soc_names (occupations)
        soc_code_field_list: List of field names which holds soc_codes
        certification_status_field_list: List of field names which holds certification status
        case_number_field_list: List of field names which hold identifier of the application

    Returns:
        data (list): Top n occupations for certified visa applications (List of Dictionaries)
    Raises:
        ValueError: Error occurred while fetching top occupations
    """
    try:
        top_occupations = 'TOP_OCCUPATIONS'
        n_applications = 'NUMBER_CERTIFIED_APPLICATIONS'
        percent = 'PERCENTAGE'
        # If the column has more than one field in the data table, (pw_soc_code and lca_soc_code)
        # we assign value from secondary fields to primary field if primary field has null value or have empty string
        if len(case_number_field_list)>1:
            for x in data:
                for i in range(1, len(case_number_field_list)):
                    final_case_number = x[case_number_field_list[i]] if x[case_number_field_list[0]] is None or x[case_number_field_list[0]] == '' else x[case_number_field_list[0]]
                    x.update({case_number_field_list[0]: final_case_number})

        if len(certification_status_field_list) > 1:
            for x in data:
                for i in range(1, len(certification_status_field_list)):
                    final_status_value = x[certification_status_field_list[i]] if x[certification_status_field_list[0]] is None or x[certification_status_field_list[0]] == '' else x[certification_status_field_list[0]]
                    x.update({certification_status_field_list[0]: final_status_value})

        if len(soc_code_field_list) > 1:
            for x in data:
                for i in range(1, len(soc_code_field_list)):
                    if x[soc_code_field_list[0]] is None or x[soc_code_field_list[0]] == '':
                        final_soc_code_field = x[soc_code_field_list[i]]
                        final_soc_name_field = x[soc_name_field_list[i]]
                        x.update({soc_code_field_list[0]: final_soc_code_field})
                        x.update({soc_name_field_list[0]: final_soc_name_field})

        filtered_data = query_load(data, lambda r: r[certification_status_field_list[0]] == "CERTIFIED"
                                                   and r[soc_code_field_list[0]] is not None and r[soc_code_field_list[0]] != ''
                                                   and r[soc_name_field_list[0]] is not None and r[soc_name_field_list[0]] != '',
                          [case_number_field_list[0], soc_name_field_list[0]], [soc_name_field_list[0]], None)

        grouped_data = []
        for k, v in groupby(filtered_data, key=lambda x: x[soc_name_field_list[0]]):
            part = len(list(v))
            whole = len(filtered_data)
            grouped_data.append({top_occupations: k, n_applications: part, percent: str(percentage(part, whole))+"%"})

        sorted_data = query_load(grouped_data, None, None, (n_applications, top_occupations),
                     ('Descending', 'Ascending'))
        return [top_occupations, n_applications, percent], sorted_data[:count]
    except Exception as err:
        logger.exception("Error occurred while fetching top occupations")
        raise err

def get_top_states(count, data, work_state_field_list, certification_status_field_list, case_number_field_list):
    """Top n occupations for certified visa applications

        Args:
            count (integer): Part of the total whose percent is calculated
            data (list): List of dictionaries
            work_state_field_list: List of field names which holds work state of employees
            certification_status_field_list: List of field names which hold certification status
            case_number_field_list: List of field names which hold identifier of the application

        Returns:
            data (list): Top n states for certified visa applications (List of Dictionaries)
        Raises:
            ValueError: Error occurred while fetching top states
        """
    try:
        top_states = 'TOP_STATES'
        n_applications = 'NUMBER_CERTIFIED_APPLICATIONS'
        percent = 'PERCENTAGE'

        # If the column has more than one entry in the data table,
        # we assign value from secondary fields to primary field if primary field has null value or have empty string
        if len(work_state_field_list)>1:
            for x in data:
                for i in range(1, len(work_state_field_list)):
                    final_state_name = x[work_state_field_list[i]] if x[work_state_field_list[0]] is None or x[work_state_field_list[0]] =='' else x[work_state_field_list[0]]
                    x.update({work_state_field_list[0]: final_state_name})

        if len(case_number_field_list) > 1:
            for x in data:
                for i in range(1, len(case_number_field_list)):
                    final_case_number = x[case_number_field_list[i]] if x[case_number_field_list[0]] is None or x[case_number_field_list[0]] =='' else x[case_number_field_list[0]]
                    x.update({case_number_field_list[0]: final_case_number})

        if len(certification_status_field_list)>1:
            for x in data:
                for i in range(1, len(certification_status_field_list)):
                    final_status_value = x[certification_status_field_list[i]] if x[certification_status_field_list[0]] is None or x[certification_status_field_list[0]] =='' else x[certification_status_field_list[0]]
                    x.update({certification_status_field_list[0]: final_status_value})

        filtered_data = query_load(data, lambda r: r[certification_status_field_list[0]] == "CERTIFIED"
                                                   and r[work_state_field_list[0]] is not None and r[work_state_field_list[0]] != '',
                                   [case_number_field_list[0], work_state_field_list[0]], [work_state_field_list[0]], None)

        grouped_data = []
        for k, v in groupby(filtered_data, key=lambda y: y[work_state_field_list[0]]):
            part = len(list(v))
            whole = len(filtered_data)
            grouped_data.append({top_states: k, n_applications: part,
                                 percent: str(percentage(part, whole))+"%"})

        sorted_data = query_load(grouped_data, None, None, (n_applications, top_states),
                     ('Descending', 'Ascending'))
        return [top_states, n_applications, percent], sorted_data[:count]
    except Exception as err:
        logger.exception("Error occurred while fetching top states")
        raise err


def main():
    try:
        # Skip script and accept 3 parameters
        argv = sys.argv[1:]
        if len(argv) !=3:
            raise ValueError("This script takes 3 arguments: 1 input file path followed by 2 output file paths")
            sys.exit(1)

        # Load the data from csv file
        headers, data = load_data(argv[0])

        # Identify field names needed for generating output
        soc_name_field_list = [k for k in headers if k in static_soc_name_field_list]
        soc_code_field_list = [k for k in headers if k in static_soc_code_field_list]
        certification_status_field_list = [k for k in headers if k in static_certification_status_field_list]
        case_number_field_list = [k for k in headers if k in static_case_number_field_list]
        work_state_field_list = [k for k in headers if k in static_work_state_field_list]

        # If matching from the static list fails, use regex to identify field names
        soc_name_field_list = list(filter(re.compile('.*(soc).*_(title|name)$').match,
                                            headers)) if not soc_name_field_list else soc_name_field_list
        soc_code_field_list = list(filter(re.compile('.*(soc).*_(code)$').match,
                                            headers)) if not soc_code_field_list else soc_code_field_list
        certification_status_field_list = list(filter(re.compile('.*(case|approval).*status$').match,
                                            headers)) if not certification_status_field_list else certification_status_field_list
        case_number_field_list = list(filter(re.compile('.*case.*_(number|no)$').match,
                                            headers)) if not case_number_field_list else case_number_field_list

        work_state_field_list = list(filter(re.compile('.*(work|worksite).*_state$').match, headers)) if not work_state_field_list else work_state_field_list


        # Checking if we found the required fields and their length
        # Length of all fields should be 1 except work_state_field which can have more than 1 field
        if len(soc_name_field_list) == 0 or len(soc_code_field_list) == 0 or len(certification_status_field_list) == 0 \
                or len(case_number_field_list) == 0 or len(work_state_field_list) == 0 or len(soc_name_field_list) != len(soc_code_field_list):
            raise ValueError("Could not find the required fields: SOC Name, SOC Code, Certification Status, Employee Work State or Case Number")

        else:
            # Call functions to get top 10 occupations and top 10  states
            occ_headers, top_occupations = get_top_occupations(10, data, soc_name_field_list,soc_code_field_list, certification_status_field_list, case_number_field_list)
            st_headers, top_states = get_top_states(10, data, work_state_field_list, certification_status_field_list, case_number_field_list)

            # Write the output to csv files
            write_data(occ_headers, top_occupations, argv[1])
            write_data(st_headers, top_states, argv[2])
    except Exception as err:
        logger.exception("Error occurred while processing the request. Below is the stack trace: \n\n")
        raise err


if __name__ == "__main__":
    main()