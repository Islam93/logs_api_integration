import sys, glob
sys.path.extend(glob.glob('/buckets/bfsdefault/default/pylib/*'))
import datetime, time
from calendar import timegm
from requests import get
import csv
import re


token = ''
counter_id = 0

metric_re = re.compile(
    '^ym:ad:.*AdCostPerVisit$|^ym:ad:.*AdCost$|^ym:ad:clicks$|^ym:ad:goal.*AdCostPerVisit$'
    '|^ym:ad:goal.*CPA$|^ym:ad:visits$|^ym:pv:blockedPercentage$|^ym:pv:cookieEnabledPercentage$'
    '|^ym:pv:flashEnabledPercentage$|^ym:pv:javaEnabledPercentage$|^ym:pv:jsEnabledPercentage$|^ym:pv:mobilePercentage$'
    '|^ym:pv:pageviewsPerDay$|^ym:pv:pageviewsPerHour$|^ym:pv:pageviewsPerMinute$'
    '|^ym:pv:pageviews$|^ym:pv:silverlightEnabledPercentage$|^ym:pv:users$|^ym:s:GCLIDPercentage$'
    '|^ym:s:affinityIndexInterests$|^ym:s:anyGoalConversionRate$|^ym:s:avgDaysBetweenVisits$'
    '|^ym:s:avgDaysSinceFirstVisit$|^ym:s:avgParams$|^ym:s:avgVisitDurationSeconds$|^ym:s:blockedPercentage$'
    '|^ym:s:bounceRate$|^ym:s:cookieEnabledPercentage$|^ym:s:ecommercePurchases$|^ym:s:ecommerceRevenuePerPurchase$'
    '|^ym:s:ecommerceRevenuePerVisit$|^ym:s:ecommerceRevenue$|^ym:s:flashEnabledPercentage$|^ym:s:goal.*revenue$'
    '|^ym:s:goal.*conversionRate$|^ym:s:goal.*reachesPerUser$|^ym:s:goal.*reaches$|^ym:s:goal.*revenue$'
    '|^ym:s:goal.*userConversionRate$|^ym:s:goal.*users$|^ym:s:goal.*visits$|^ym:s:javaEnabledPercentage$'
    '|^ym:s:jsEnabledPercentage$|^ym:s:manPercentage$|^ym:s:mobilePercentage$|^ym:s:newUserVisitsPercentage$'
    '|^ym:s:newUsers$|^ym:s:offlineCallDurationAvg$|^ym:s:offlineCallHoldDurationTillAnswerAvg$'
    '|^ym:s:offlineCallHoldDurationTillMissAvg$|^ym:s:offlineCallRevenueAvg$|^ym:s:offlineCallRevenue$'
    '|^ym:s:offlineCallTalkDurationAvg$|^ym:s:offlineCallsFirstTimeCallerPercentage$|^ym:s:offlineCallsFirstTimeCaller$'
    '|^ym:s:offlineCallsMissedPercentage$|^ym:s:offlineCallsMissed$|^ym:s:offlineCallsUniq$|^ym:s:offlineCalls$'
    '|^ym:s:oneDayBetweenVisitsPercentage$|^ym:s:oneVisitPerUserPercentage$|^ym:s:over32VisitsPerUserPercentage$'
    '|^ym:s:over54AgePercentage$|^ym:s:overMonthBetweenVisitsPercentage$|^ym:s:overYearSinceFirstVisitPercentage$'
    '|^ym:s:overYearUserRecencyPercentage$|^ym:s:pageDepth$|^ym:s:pageviews$|^ym:s:paramsNumber$'
    '|^ym:s:percentNewVisitors$|^ym:s:productBasketsPrice$|^ym:s:productBasketsQuantity$|^ym:s:productBasketsUniq$'
    '|^ym:s:productImpressionsUniq$|^ym:s:productImpressions$|^ym:s:productPurchasedPrice$'
    '|^ym:s:productPurchasedQuantity$|^ym:s:productPurchasedUniq$|^ym:s:pvlAll<offline_window>Window$'
    '|^ym:s:pvl.*Point.*Window$|^ym:s:pvl.*Region.*Window$|^ym:s:robotPercentage$|^ym:s:silverlightEnabledPercentage$'
    '|^ym:s:sumGoalReachesAny$|^ym:s:sumParams$|^ym:s:under18AgePercentage$|^ym:s:upTo24AgePercentage$'
    '|^ym:s:upTo31VisitsPerUserPercentage$|^ym:s:upTo34AgePercentage$|^ym:s:upTo3VisitsPerUserPercentage$'
    '|^ym:s:upTo44AgePercentage$|^ym:s:upTo54AgePercentage$|^ym:s:upTo7VisitsPerUserPercentage$'
    '|^ym:s:upToDaySinceFirstVisitPercentage$|^ym:s:upToDayUserRecencyPercentage$'
    '|^ym:s:upToMonthBetweenVisitsPercentage$|^ym:s:upToMonthSinceFirstVisitPercentage$'
    '|^ym:s:upToMonthUserRecencyPercentage$|^ym:s:upToQuarterSinceFirstVisitPercentage$'
    '|^ym:s:upToQuarterUserRecencyPercentage$|^ym:s:upToWeekBetweenVisitsPercentage$'
    '|^ym:s:upToWeekSinceFirstVisitPercentage$|^ym:s:upToWeekUserRecencyPercentage$'
    '|^ym:s:upToYearSinceFirstVisitPercentage$|^ym:s:upToYearUserRecencyPercentage$|^ym:s:userRecencyDays$'
    '|^ym:s:users$|^ym:s:visitsPerDay$|^ym:s:visitsPerHour$|^ym:s:visitsPerMinute$|^ym:s:visits$|^ym:s:womanPercentage$'
    '|^ym:s:yanARPU$|^ym:s:yanCPMV$|^ym:s:yanECPM$|^ym:s:yanPartnerPrice$|^ym:s:yanRPM$|^ym:s:yanRendersPerHit$'
    '|^ym:s:yanRendersPerUser$|^ym:s:yanRendersPerVisit$|^ym:s:yanRenders$|^ym:s:yanRequests$|^ym:s:yanRevenuePerHit$'
    '|^ym:s:yanRevenuePerVisit$|^ym:s:yanShows$|^ym:s:yanVisibility$'
)

date1 = '2020-04-15'
date2 = '2020-04-15'

# avia_uniq_main_vasits.json
filters = "ym:pv:URL=@'avia.tutu.ru'"
# uniq_users_from_tutu_poezda.json
# filters = "ym:pv:URL=*'*tutu.ru//poezda' AND ym:s:startURL=*'tutu.ru//poezda'"
# uniq_users.json
# filters = ""


# avia_uniq_main_vasits.json
columns = [
    'ym:s:date',
    'ym:s:lastTrafficSource',
    'ym:s:deviceCategory',
    "ym:s:users"
]

# uniq_users_from_tutu_poezda.json
# columns = [
#     'ym:s:trafficSource',
#     'ym:s:searchEngine',
#     'ym:s:advEngine',
#     'ym:s:isNewUser',
#     'ym:s:deviceCategory',
#     "ym:s:users"
# ]
# uniq_users.json
# columns = [
#     'ym:s:trafficSource',
#     'ym:s:searchEngine',
#     'ym:s:advEngine',
#     'ym:s:isNewUser',
#     'ym:s:deviceCategory',
#     "ym:s:users"
# ]

metrics = []
dimensions = []

fields = columns
for f in fields:
    if metric_re.match(f):
        metrics.append(f)
    else:
        dimensions.append(f)

hdr = {'Authorization': 'OAuth ' + token, 'Content-Type': 'application/x-yametrika+json'}
params = {
    'ids': counter_id,
    'metrics': metrics,
    'dimensions': dimensions,
    'date1': date1,
    'date2': date2,
    'limit': 10000,
    'include_undefined': True,
    'filters': filters,
    'accuracy': 'full',
    'group': 'Day'
}
# r = get('https://api-metrika.yandex.net/stat/v1/data.csv', params, headers=hdr)
r = get('https://api-metrika.yandex.net/stat/v1/data', params, headers=hdr)
if r.status_code != 200:
    raise (Exception(r.status_code, r.text))

with open('/Users/israfilov/src/logs_api_integration/log/ag.json', 'w') as file:
    file.write(r.content.decode('utf-8'))

# skip = 1 if len(dimensions) == 0 else 2
# for i, row in enumerate(csv.reader(r.content[4:].splitlines(), delimiter=",")):
#     if i >= skip:
#         print(row)










