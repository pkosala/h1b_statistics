import csv
import sys
from operator import itemgetter
from functools import cmp_to_key
from itertools import groupby
import re
import logging
logger = logging.getLogger(__name__)

def load_data(filename):
    """Load data from a csv file.

    Args:
      filename(str): filename to read.
    Returns:
      (headers, data): Columns name of the data and the data.
    Raises:
      ValueError: Unable to read file
    """
    data = []
    try:
        with open(filename, newline='', encoding="utf8") as fromfile:
            reader = csv.reader(fromfile, delimiter=';')
            headers = next(reader, None)
            rdr = csv.DictReader(fromfile, headers, delimiter=';', quotechar='"')
            data.extend(rdr)
    except IOError as err:
        logger.exception('Error occurred while reading the data from file: ' + filename)
        raise err
    if not data:
        raise ValueError('No data available')
    return headers, data

def write_data(data, filename):
    """Write data to a csv file.

    Args:
        data (list): List of dictionaries
        filename(str): Filename to write to.
    Returns:
        (headers, data): Columns name of the data and the data.
    Raises:
        ValueError: Unable to write to file
                    No data to write error

    """
    try:
        if data is None:
            raise ValueError("No data to write")
        keys = data[0].keys()
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


def get_top_occupations(count, data, soc_name_field,soc_code_field, certification_status_field, case_number_field):
    """Top n occupations for certified visa applications

    Args:
        count (integer): Part of the total whose percent is calculated
        data (list): List of dictionaries
        soc_name_field: Field name which holds soc_names (occupations)
        soc_code_field: Field name which holds soc_codes
        certification_status_field: Field name which holds certification status
        case_number_field: Identifier of the application

    Returns:
        data (list): Top n occupations for certified visa applications (List of Dictionaries)
    Raises:
        ValueError: Error occurred while fetching top occupations
    """
    try:
        filtered_data = query_load(data, lambda r: r[certification_status_field] == "CERTIFIED" and r[soc_code_field] is not None,
                          [case_number_field, soc_name_field], [soc_name_field], None)

        grouped_data = []
        for k, v in groupby(filtered_data, key=lambda x: x[soc_name_field]):
            part = len(list(v))
            whole = len(filtered_data)
            grouped_data.append({'TOP_OCCUPATIONS': k, 'NUMBER_CERTIFIED_APPLICATIONS': part,
                         'PERCENTAGE': str(percentage(part, whole))+"%"})

        sorted_data = query_load(grouped_data, None, None, ('NUMBER_CERTIFIED_APPLICATIONS', 'TOP_OCCUPATIONS'),
                     ('Descending', 'Ascending'))
        return sorted_data[:count]
    except Exception as err:
        logger.exception("Error occurred while fetching top occupations")
        raise err

def get_top_states(count, data, work_state_field_list, certification_status_field, case_number_field):
    """Top n occupations for certified visa applications

        Args:
            count (integer): Part of the total whose percent is calculated
            data (list): List of dictionaries
            work_state_field_list: List of field names which holds work state of employees
            certification_status_field: Field name which holds certification status
            case_number_field: Identifier of the application

        Returns:
            data (list): Top n states for certified visa applications (List of Dictionaries)
        Raises:
            ValueError: Error occurred while fetching top states
        """
    try:
        filtered_data = query_load(data, lambda r: r[certification_status_field] == "CERTIFIED",
                            [case_number_field]+work_state_field_list, work_state_field_list, None)

        work_state_field_list = list(sorted(work_state_field_list))
        # Employees may have more than 1 work location, we sort these field by title
        # and assign work locations from secondary fields to primary field if primary field has null value or have empty string
        for x in filtered_data:
            for i in range(1, len(work_state_field_list)):
                final_state_name = x[work_state_field_list[i]] if x[work_state_field_list[0]] is None or x[work_state_field_list[0]] =='' else x[work_state_field_list[0]]
                x.update({work_state_field_list[0]: final_state_name})

        grouped_data = []
        for k, v in groupby(filtered_data, key=lambda y: y[work_state_field_list[0]]):
            part = len(list(v))
            whole = len(filtered_data)
            grouped_data.append({'TOP_STATES': k, 'NUMBER_CERTIFIED_APPLICATIONS': part,
                         'PERCENTAGE': str(percentage(part, whole))+"%"})

        sorted_data = query_load(grouped_data, None, None, ('NUMBER_CERTIFIED_APPLICATIONS', 'TOP_STATES'),
                     ('Descending', 'Ascending'))
        return sorted_data[:count]
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
        soc_name_field_list = [k for k in headers if 'SOC_NAME' in k or 'SOC_TITLE' in k]
        soc_code_field_list = [k for k in headers if 'SOC_CODE' in k]
        certification_status_field_list = [k for k in headers if 'STATUS' in k]
        case_number_field_list = [k for k in headers if 'CASE_NUMBER' in k or 'CASE_NO' in k]
        work_state_field_list = list(filter(re.compile('.*WORK.*_STATE$').match, headers))

        # Checking if we found the required fields and their length
        # Length of all fields should be 1 except work_state_field which can have more than 1 field
        if len(soc_name_field_list) == 0 or len(soc_code_field_list) == 0 or len(certification_status_field_list) == 0 \
                or len(case_number_field_list) == 0 or len(work_state_field_list) == 0:
            raise ValueError("Could not find the required fields: SOC Name, SOC Code, Certification Status, Employee Work State or Case Number")
        elif len(soc_name_field_list) >1 or len(soc_code_field_list) >1 or len(certification_status_field_list) >1 \
                or len(case_number_field_list) > 1:
            raise ValueError("Found more than 1 similar fields for: SOC Name, SOC Code, Certification Status, Employee Work State or Case Number")
        else:
            # Call functions to get top 10 occupations and top 10  states
            top_occupations = get_top_occupations(10, data, soc_name_field_list[0],soc_code_field_list[0], certification_status_field_list[0], case_number_field_list[0])
            top_states = get_top_states(10, data, work_state_field_list, certification_status_field_list[0], case_number_field_list[0])

            # Write the output to csv files
            write_data(top_occupations, argv[1])
            write_data(top_states, argv[2])
    except Exception as err:
        logger.exception("Error occurred while processing the request. Below is the stack trace: \n\n")
        raise err


if __name__ == "__main__":
    main()