import re

def replace_insert_with_count(sql_query):
    # Define the regular expressions to find and replace
    insert_pattern = r'INSERT *(.+?\s+\SELECT\b)'


    # Define the replacement strings
    if 'WITH ' in sql_query:
        insert_replace = ', final_table AS ( \nselect'
    elif 'with ' in sql_query:
        insert_replace = ', final_table AS ( \nselect'
    else:
        insert_replace = 'WITH final_table AS ( \nselect'


    # Apply the regular expressions and replacements
    sql_query = re.sub(insert_pattern, insert_replace, sql_query, flags=re.IGNORECASE)
    sql_query = sql_query + ')\nselect count(*) from final_table'
    
    return sql_query