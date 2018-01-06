import requests
from images6.job import JobFeed
from images6.job import raw, jpg, imageproxy
from images6.job import State


r = requests.get('http://localhost:8888/job?page_size=20')

jobs = JobFeed.FromDict(r.json())

for job in jobs.entries:
    print("%10d  %5d  %20s %15s" % (job.id, job.options.entry_id, job.method, job.state.value))

stats = jobs.stats

print("")
print("%d jobs (%d new, %d acquired, %d active, %d held, %d done and %d failed)" % (stats.total, stats.new, stats.acquired, stats.active, stats.held, stats.done, stats.failed))
