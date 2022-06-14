from collections import defaultdict
import boto3
import datetime
import os
import requests

n_days = 7

# It seems that the sparkline symbols don't line up (probably based on font?) so put them last
# Also, leaving out the full block because Slack doesn't like it: '█'
sparks = ['▁', '▂', '▃', '▄', '▅', '▆', '▇']

short_names = {
    "Amazon Relational Database Service": "RDS",
    "Amazon Elastic Compute Cloud - Compute": "EC2 - Compute",
    "Savings Plans for AWS Compute usage": "Savings Plans",
    "Amazon Simple Storage Service": "S3",
}

def sparkline(datapoints):
    lower = min(datapoints)
    upper = max(datapoints)
    width = upper - lower
    n_sparks = len(sparks) - 1

    line = ""
    for dp in datapoints:
        scaled = 1 if width == 0 else (dp - lower) / width
        which_spark = int(scaled * n_sparks)
        line += (sparks[which_spark])

    return line


def delta(costs):
    if (len(costs) > 1 and costs[-1] >= 1 and costs[-2] >= 1):
        # This only handles positive numbers
        result = ((costs[-1]/costs[-2])-1)*100.0
    else:
        result = 0
    return result


def find_by_key(values: list, key: str, value: str):
    for item in values:
        if item.get(key) == value:
            return item
    return None


def lambda_handler(event, context):
    group_by = os.environ.get("GROUP_BY", "SERVICE")
    length = int(os.environ.get("LENGTH", "5"))

    summary, buffer, data = report_cost(group_by=group_by, length=length)

    slack_hook_url = os.environ.get('SLACK_WEBHOOK_URL')
    if slack_hook_url:
        publish_slack(slack_hook_url, summary, buffer)

    teams_hook_url = os.environ.get('TEAMS_WEBHOOK_URL')
    if teams_hook_url:
        publish_teams(teams_hook_url, summary, buffer)


def report_cost(group_by: str = "SERVICE", length: int = 5, result: dict = None):

    today = datetime.datetime.today()
    yesterday = today - datetime.timedelta(days=1)
    report_calculation_day = yesterday - datetime.timedelta(days=1)
    week_ago = today - datetime.timedelta(days=n_days)
    # Generate list of dates, so that even if our data is sparse,
    # we have the correct length lists of costs (len is n_days)
    list_of_dates = [
        (week_ago + datetime.timedelta(days=x)).strftime('%Y-%m-%d')
        for x in range(n_days)
    ]
    print(list_of_dates)

    # Get account account name from env, or account id/account alias from boto3
    account_name = os.environ.get("AWS_ACCOUNT_NAME", None)
    if account_name is None:
        iam = boto3.client("iam")
        paginator = iam.get_paginator("list_account_aliases")
        for aliases in paginator.paginate(PaginationConfig={"MaxItems": 1}):
            if "AccountAliases" in aliases and len(aliases["AccountAliases"]) > 0:
                account_name = aliases["AccountAliases"][0]

    if account_name is None or account_name == "":
        account_name = boto3.client("sts").get_caller_identity().get("Account")

    if account_name is None or account_name == "":
        account_name = "[NOT FOUND]"

    client = boto3.client('ce')

    query = {
        "TimePeriod": {
            "Start": week_ago.strftime('%Y-%m-%d'),
            "End": yesterday.strftime('%Y-%m-%d'),
        },
        "Granularity": "DAILY",
        "Filter": {
            "Not": {
                "Dimensions": {
                    "Key": "RECORD_TYPE",
                    "Values": [
                        "Credit",
                        "Refund",
                        "Upfront",
                        "Support",
                        "Tax",
                    ]
                }
            }
        },
        "Metrics": ["UnblendedCost"],
        "GroupBy": [
            {
                "Type": "DIMENSION",
                "Key": group_by,
            },
        ],
    }

    # Only run the query when on lambda, not when testing locally with example json
    if result is None:
        result = client.get_cost_and_usage(**query)

    cost_per_day_by_service = defaultdict(list)
    cost_per_day_dict = defaultdict(dict)

    for day in result['ResultsByTime']:
        start_date = day["TimePeriod"]["Start"]
        for group in day['Groups']:
            key = group['Keys'][0]
            if group_by == "LINKED_ACCOUNT":
                dimension = find_by_key(result["DimensionValueAttributes"], "Value", key)
                if dimension:
                    key += " ("+dimension["Attributes"]["description"]+")"
            cost = float(group['Metrics']['UnblendedCost']['Amount'])
            cost_per_day_dict[key][start_date] = cost

    for key in cost_per_day_dict.keys():
        for start_date in list_of_dates:
            cost = cost_per_day_dict[key].get(start_date, 0.0)  # fallback for sparse data
            if key in short_names:
                short_name = short_names[key]
            else:
                short_name = key.removeprefix("Amazon").strip()
            cost_per_day_by_service[short_name].append(cost)

    # Sort the map by yesterday's cost
    most_expensive_yesterday = sorted(cost_per_day_by_service.items(), key=lambda i: i[1][-1], reverse=True)

    service_names = [k for k,_ in most_expensive_yesterday[:length]]
    longest_name_len = len(max(service_names, key = len))

    buffer = f"{'Service':{longest_name_len}} 📆 {yesterday.strftime('%Y-%m-%d'):11}     {'Last 7d':8}\n"

    for service_name, costs in most_expensive_yesterday[:length]:
        buffer += f"{service_name:{longest_name_len}} ${costs[-1]:12,.2f}     {sparkline(costs):8}\n"

    other_costs = [0.0] * n_days
    for service_name, costs in most_expensive_yesterday[length:]:
        for i, cost in enumerate(costs):
            other_costs[i] += cost

    buffer += f"{'Other':{longest_name_len}} ${other_costs[-1]:12,.2f}     {sparkline(other_costs):8}\n"

    total_costs = [0.0] * n_days
    for day_number in range(n_days):
        for service_name, costs in most_expensive_yesterday:
            try:
                total_costs[day_number] += costs[day_number]
            except IndexError:
                total_costs[day_number] += 0.0
    buffer +=  f"----------------------------------------------\n"
    buffer += f"{'👉 Total':{longest_name_len-1}} ${total_costs[-1]:12,.2f}     {sparkline(total_costs):8}"

    cost_per_day_by_service["total"] = total_costs[-1]
    daily_budget = os.environ.get('DAILY_BUDGET')
    if daily_budget:
        if total_costs[-1] < float(daily_budget):
            emoji = ":white_check_mark:"
        else:
            emoji = ":rotating_light:"
        summary = f"{emoji} {report_calculation_day.strftime('%a %-d. of %b, %Y')}: Yesterday's cost for AWS was *${total_costs[-1]:,.2f}* compared to target daily budget of *${daily_budget}*."
    else:
        summary = f"{report_calculation_day.strftime('%a %-d. of %b, %Y')}: Yesterday's cost for AWS was *${total_costs[-1]:,.2f}*."

    return summary, buffer, cost_per_day_by_service


def publish_slack(hook_url, summary, buffer):

    resp = requests.post(
        hook_url,
        json={
            "text": summary + "\n\n```\n" + buffer + "\n```",
        }
    )

    if resp.status_code != 200:
        print("HTTP %s: %s" % (resp.status_code, resp.text))


def publish_teams(hook_url, summary, buffer):

    resp = requests.post(
        hook_url,
        json={
            "text": summary + "\n\n```\n" + buffer + "\n```",
        }
    )

    if resp.status_code != 200:
        print("HTTP %s: %s" % (resp.status_code, resp.text))


if __name__ == "__main__":
    # for running locally to test
    import json
    with open("example_boto3_result.json", "r") as f:
        example_result = json.load(f)
    with open("example_boto3_result2.json", "r") as f:
        example_result2 = json.load(f)

    # summary, buffer, data = report_cost(group_by="LINKED_ACCOUNT")
    # print(summary)
    # print(buffer)
    #
    # summary, buffer, data = report_cost(group_by="REGION")
    # print(summary)
    # print(buffer)
    #
    # summary, buffer, data = report_cost(group_by="USAGE_TYPE", length=20)
    # print(summary)
    # print(buffer)
    #
    # summary, buffer, data = report_cost(group_by="SERVICE", length=20)
    # print(summary)
    # print(buffer)

    # New Method with 2 example jsons
    summary, buffer, cost_dict = report_cost(None, None, example_result, yesterday="2021-08-23", new_method=True)
    assert "{0:.2f}".format(cost_dict.get("total", 0.0)) == "286.37", f'{cost_dict.get("total"):,.2f} != 286.37'
    summary, buffer, cost_dict = report_cost(None, None, example_result2, yesterday="2021-08-29", new_method=True)
    assert "{0:.2f}".format(cost_dict.get("total", 0.0)) == "21.45", f'{cost_dict.get("total"):,.2f} != 21.45'

    # Old Method with same jsons (will fail)
    summary, buffer, cost_dict = report_cost(None, None, example_result, yesterday="2021-08-23", new_method=False)
    assert "{0:.2f}".format(cost_dict.get("total", 0.0)) == "286.37", f'{cost_dict.get("total"):,.2f} != 286.37'
    summary, buffer, cost_dict = report_cost(None, None, example_result2, yesterday="2021-08-29", new_method=False)
    assert "{0:.2f}".format(cost_dict.get("total", 0.0)) == "21.45", f'{cost_dict.get("total"):,.2f} != 21.45'
