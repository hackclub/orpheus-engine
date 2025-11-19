This is a set of Python scripts that generates dashboards and reporting across Hack Club. 

Most nonprofits / organizations / companies don’t have the code for their data pipelines public. I’ve actually never seen anyone publicize it before besides Dagster’s sample project. All of Hack Club's data scripts are open source in this repo.

Some other tasks it does are:

- Calculates sign-up counts and HCB spend for YSWS programs
- Downloads our email list and backs it up in Postgres
- Syncs Airtables to Postgres for reporting

To illustrate, here’s what happens when you submit a project that gets approved for YSWS:

- If we don’t already have a gender on file for you, we use the Genderize API to try and guess your gender using the name you provided. We use this to try and understand how many girls are shipping projects, since we don’t want to have to ask for gender across every form and care about helping more girls make projects. If you have self-reported gender before, we always default to your self reported gender (male / female / or nonbinary)

- We geocode your address using the Google Maps API to convert it into a latitude and longitude. We use this for map visualizations and also to understand how many Hack Clubbers are participating from different regions (ex. USA vs. Europe).

- We use the GitHub API to get the current number of stars from your GitHub repo. If your repo has more than 5 GitHub stars, we then use the Bright Data API to try and find places your project has gone viral on the internet (ex. reddit / Hacker News / etc). We do this as one way of understanding quality of projects across YSWS programs. For example, if a YSWS program has 1,000 projects made for it and none went viral - that’s a red flag. Or, if a smaller YSWS program has a lot of projects that went viral, then something amazing happened and we need to understand what worked so we can do it more. This is new and an attempt to start measuring quality of projects instead of just quantity. I think it’d be cool if this could be shared with project authors too (ex. “your project just went viral!” or a place where you can see all the places your projects went viral)

- We use archive.hackclub.com (https://github.com/hackclub/arker) to archive the public deployed page for your project and your git repo. Occasionally this is helpful for fraud detection (sometimes people ship a project, get approved, them immediately private their repo because they did fraud and we can’t retroactively go and check to see what was in their repo once it’s gone). It’s also for generally a historic record - some of the coolest projects made years ago in Hack Club are sadly no longer online and it’s sad that we can no longer see them. This only pulls public information from your public git repo and your public deployed page. I’m planning to add Wayback Machine support for this too.

