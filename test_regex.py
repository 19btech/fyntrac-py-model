import re

code = """
        EOD_postingdate = EOD_postingdate
        EOD_effectivedate = EOD_effectivedate
        EOD_subinstrumentid = EOD_subinstrumentid
        EOD_BILLING = EOD_BILLING
        EOD_UNBILLED = EOD_UNBILLED
        EOD_REVENUE = collect_by_instrument('EOD_REVENUE')
"""

fixed_code = re.sub(r'^\s*([A-Za-z0-9_]+)\s*=\s*\1\s*$', r"        \1 = get_field_case_insensitive(row, '\1', '')", code, flags=re.MULTILINE)
print(fixed_code)
