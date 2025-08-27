#!/usr/bin/env python3
"""
GitHub Commit Statistics Calculator - v2

Uses GitHub's built-in /stats/contributors API for accurate statistics
"""

import os
import sys
import requests
import asyncio
import aiohttp
from urllib.parse import urlparse
from typing import Dict, List, Any, Optional
import json
import time
from dotenv import load_dotenv

# Load environment variables from .env file in parent directory
load_dotenv('../.env')

class GitHubCommitStatsV2:
    def __init__(self, api_token: str):
        self.api_token = api_token
        self.base_url = "https://gh-proxy.hackclub.com/gh"
        self.headers = {
            "X-API-Key": api_token,
            "Accept": "application/vnd.github+json"
        }
        
    def extract_repo_info(self, repo_url: str) -> tuple[str, str]:
        """Extract owner and repo name from GitHub URL"""
        parsed = urlparse(repo_url)
        path_parts = parsed.path.strip('/').split('/')
        if len(path_parts) >= 2:
            owner = path_parts[0]
            repo = path_parts[1].replace('.git', '')
            return owner, repo
        else:
            raise ValueError(f"Invalid GitHub URL format: {repo_url}")
    
    def get_contributors_stats(self, owner: str, repo: str, max_retries: int = 8) -> List[Dict[str, Any]]:
        """Get contributor statistics from GitHub API with exponential backoff"""
        url = f"{self.base_url}/repos/{owner}/{repo}/stats/contributors"
        
        for attempt in range(max_retries):
            print(f"Fetching contributor stats (attempt {attempt + 1}/{max_retries})...")
            response = requests.get(url, headers=self.headers)
            
            if response.status_code == 404:
                raise ValueError(f"Repository {owner}/{repo} not found or not accessible")
            elif response.status_code == 403:
                raise ValueError("API rate limit exceeded or insufficient permissions")
            elif response.status_code == 422:
                # Repository has >10k commits, GitHub can't provide detailed stats
                print("Repository has >10,000 commits - GitHub API limitation")
                return None
            elif response.status_code == 202:
                # GitHub is still computing stats, wait with exponential backoff
                wait_time = min(2 ** attempt, 60)  # Exponential backoff, max 60 seconds
                print(f"GitHub is computing statistics, waiting {wait_time} seconds (attempt {attempt + 1})...")
                time.sleep(wait_time)
                continue
            elif response.status_code == 200:
                return response.json()
            else:
                raise ValueError(f"API error: {response.status_code} - {response.text}")
        
        raise ValueError("Failed to get statistics after maximum retries (GitHub still computing)")
    
    async def get_commit_stats_async(self, session: aiohttp.ClientSession, owner: str, repo: str, commit_sha: str, debug: bool = False) -> Dict[str, int]:
        """Get detailed stats for a specific commit asynchronously"""
        url = f"{self.base_url}/repos/{owner}/{repo}/commits/{commit_sha}"
        
        async with session.get(url, headers=self.headers) as response:
            if response.status != 200:
                return {"additions": 0, "deletions": 0, "total": 0, "sha": commit_sha}
            
            commit_data = await response.json()
            stats = commit_data.get("stats", {})
            files = commit_data.get("files", [])
            
            # Debug: check for binary files or unusual changes
            if debug:
                binary_count = sum(1 for f in files if f.get("status") == "binary")
                if binary_count > 0:
                    print(f"Commit {commit_sha[:8]} has {binary_count} binary files")
            
            return {
                "additions": stats.get("additions", 0),
                "deletions": stats.get("deletions", 0), 
                "total": stats.get("total", 0),
                "sha": commit_sha,
                "file_count": len(files),
                "binary_files": sum(1 for f in files if f.get("status") == "binary")
            }
    
    async def get_all_commits_async(self, session: aiohttp.ClientSession, owner: str, repo: str) -> List[Dict[str, Any]]:
        """Get all commits from the repository asynchronously"""
        commits = []
        page = 1
        per_page = 100
        
        print(f"Fetching commits from {owner}/{repo}...")
        
        while True:
            url = f"{self.base_url}/repos/{owner}/{repo}/commits"
            params = {"page": page, "per_page": per_page}
            
            async with session.get(url, headers=self.headers, params=params) as response:
                if response.status == 404:
                    raise ValueError(f"Repository {owner}/{repo} not found or not accessible")
                elif response.status == 403:
                    raise ValueError("API rate limit exceeded or insufficient permissions")
                elif response.status != 200:
                    raise ValueError(f"API error: {response.status} - {await response.text()}")
                
                page_commits = await response.json()
                if not page_commits:
                    break
                    
                commits.extend(page_commits)
                print(f"Fetched {len(commits)} commits so far...")
                page += 1
                
                # Safety check to avoid infinite loops
                if page > 200:  # Max ~20k commits
                    print("Warning: Reached maximum page limit, there may be more commits")
                    break
                    
        return commits
    
    async def calculate_line_changes_manually(self, owner: str, repo: str, username: str) -> Dict[str, Any]:
        """Calculate line changes manually using individual commit API calls"""
        start_time = time.time()
        connector = aiohttp.TCPConnector(limit=100, limit_per_host=50)
        timeout = aiohttp.ClientTimeout(total=300)  # 5 minute timeout
        
        async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
            # Get all commits
            commits = await self.get_all_commits_async(session, owner, repo)
            
            # Filter commits to match GitHub's stats API logic
            # GitHub excludes: merge commits and empty commits
            filtered_commits = []
            user_commits = []
            
            for commit in commits:
                # Skip merge commits (commits with multiple parents)
                commit_data = commit.get("commit", {})
                parents = commit.get("parents", [])
                if len(parents) > 1:
                    continue  # Skip merge commits
                
                filtered_commits.append(commit)
                
                # Check if this commit belongs to the user
                commit_author = commit.get("author")
                committer = commit.get("committer")
                
                # Check GitHub user login
                author_login = commit_author.get("login") if commit_author else None
                committer_login = committer.get("login") if committer else None
                
                # Also check commit author/committer name and email
                commit_author_data = commit_data.get("author", {})
                commit_committer_data = commit_data.get("committer", {})
                commit_author_name = commit_author_data.get("name", "")
                commit_author_email = commit_author_data.get("email", "")
                commit_committer_name = commit_committer_data.get("name", "")
                commit_committer_email = commit_committer_data.get("email", "")
                
                if (author_login == username or committer_login == username or
                    username.lower() in commit_author_name.lower() or
                    username.lower() in commit_author_email.lower() or
                    username.lower() in commit_committer_name.lower() or
                    username.lower() in commit_committer_email.lower()):
                    user_commits.append(commit)
            
            print(f"Filtered out {len(commits) - len(filtered_commits)} merge commits")
            print(f"GitHub API reports {len(commits)} total commits, we have {len(filtered_commits)} after filtering")
            commits = filtered_commits
            
            print(f"Found {len(user_commits)} commits by {username} out of {len(commits)} total commits")
            print(f"Fetching detailed stats for all commits (this may take a few minutes)...")
            
            # Create semaphore to limit concurrent requests (100/sec = ~100 concurrent)
            semaphore = asyncio.Semaphore(100)
            
            async def get_commit_with_semaphore(commit):
                async with semaphore:
                    return await self.get_commit_stats_async(session, owner, repo, commit["sha"], debug=False)
            
            # Batch process all commits
            all_commit_tasks = [get_commit_with_semaphore(commit) for commit in commits]
            user_commit_tasks = [get_commit_with_semaphore(commit) for commit in user_commits]
            
            print("Processing all repository commits...")
            all_results = []
            for i, task in enumerate(asyncio.as_completed(all_commit_tasks)):
                result = await task
                all_results.append(result)
                if (i + 1) % 100 == 0:
                    print(f"Processed {i + 1}/{len(all_commit_tasks)} repository commits")
            
            print("Processing user commits...")
            user_results = []
            for i, task in enumerate(asyncio.as_completed(user_commit_tasks)):
                result = await task
                user_results.append(result)
                if (i + 1) % 50 == 0:
                    print(f"Processed {i + 1}/{len(user_commit_tasks)} user commits")
            
            # Filter out empty commits (GitHub excludes these from contributor stats)
            non_empty_all_results = [r for r in all_results if r["total"] > 0]
            non_empty_user_results = [r for r in user_results if r["total"] > 0]
            
            print(f"Filtered out {len(all_results) - len(non_empty_all_results)} empty commits from repo")
            print(f"Filtered out {len(user_results) - len(non_empty_user_results)} empty commits from user")
            
            # Calculate totals (excluding empty commits for user stats)
            total_repo_additions = sum(r["additions"] for r in all_results)
            total_repo_deletions = sum(r["deletions"] for r in all_results)
            total_repo_changes = total_repo_additions + total_repo_deletions
            
            total_user_additions = sum(r["additions"] for r in non_empty_user_results)
            total_user_deletions = sum(r["deletions"] for r in non_empty_user_results)
            total_user_changes = total_user_additions + total_user_deletions
            
            # Update commit counts to exclude empty commits from user count
            final_user_commit_count = len(non_empty_user_results)
            
            end_time = time.time()
            execution_time = end_time - start_time
            total_api_calls = len(commits) + len(user_commits)
            
            print(f"\nExecution completed in {execution_time:.1f} seconds")
            print(f"Total API calls made: {total_api_calls:,}")
            print(f"Average rate: {total_api_calls / execution_time:.1f} requests/second")
            
            # Debug: show binary file stats
            total_binary_files = sum(r.get("binary_files", 0) for r in all_results)
            if total_binary_files > 0:
                print(f"Note: Repository contains {total_binary_files} binary file changes")
            
            # Use GitHub's contributor sum as the authoritative total commit count
            github_total_commits = sum(len([r for r in all_results if r["total"] > 0]) for i in range(1))  
            # Actually, let's use the sum of non-empty commits as GitHub does
            github_total_commits = len(non_empty_all_results)
            
            return {
                "repository": f"{owner}/{repo}",
                "username": username,
                "total_commits": github_total_commits,
                "user_commits": final_user_commit_count,
                "user_commit_percentage": round((final_user_commit_count / github_total_commits * 100) if github_total_commits else 0, 2),
                "total_repo_changes": total_repo_changes,
                "total_user_changes": total_user_changes,
                "user_changes_percentage": round((total_user_changes / total_repo_changes * 100) if total_repo_changes else 0, 2),
                "breakdown": {
                    "user_additions": total_user_additions,
                    "user_deletions": total_user_deletions,
                    "repo_additions": total_repo_additions,
                    "repo_deletions": total_repo_deletions
                },
                "execution_time_seconds": round(execution_time, 1),
                "total_api_calls": total_api_calls,
                "average_rate_per_second": round(total_api_calls / execution_time, 1),
                "data_source": "Manual calculation matching GitHub's logic (excludes merge commits and empty commits)"
            }
    
    async def calculate_user_stats(self, repo_url: str, username: str) -> Dict[str, Any]:
        """Calculate comprehensive commit statistics for a user using GitHub's built-in API"""
        owner, repo = self.extract_repo_info(repo_url)
        contributors_stats = self.get_contributors_stats(owner, repo)
        
        if contributors_stats is None:
            # GitHub returned 422 - repository has >10k commits
            print("GitHub's built-in stats API unavailable (>10k commits), using manual calculation...")
            return await self.calculate_line_changes_manually(owner, repo, username)
        
        if not contributors_stats:
            raise ValueError("No contributor statistics available for this repository")
        
        # Debug: print raw response structure
        print(f"Found {len(contributors_stats)} contributors")
        if contributors_stats and len(contributors_stats) > 0:
            print(f"First contributor structure: {type(contributors_stats[0])}")
            if contributors_stats[0]:
                print(f"First contributor keys: {list(contributors_stats[0].keys()) if hasattr(contributors_stats[0], 'keys') else 'No keys'}")
        
        # Find the target user in contributors
        user_stats = None
        for i, contributor in enumerate(contributors_stats):
            if contributor is None:
                print(f"Warning: Contributor {i} is None")
                continue
                
            author_info = contributor.get("author", {})
            if author_info is None:
                print(f"Warning: Contributor {i} has no author info")
                continue
                
            author_login = author_info.get("login", "")
            
            if author_login.lower() == username.lower():
                user_stats = contributor
                break
        
        if not user_stats:
            print(f"\nAvailable contributors:")
            for contributor in contributors_stats[:10]:  # Show first 10
                author_info = contributor.get("author", {})
                author_login = author_info.get("login", "unknown")
                total_commits = contributor.get("total", 0)
                print(f"  - {author_login}: {total_commits} commits")
            if len(contributors_stats) > 10:
                print(f"  ... and {len(contributors_stats) - 10} more contributors")
            
            raise ValueError(f"User '{username}' not found in repository contributors")
        
        # Calculate totals across all contributors
        total_commits_all = sum(c.get("total", 0) for c in contributors_stats)
        total_additions_all = 0
        total_deletions_all = 0
        total_changes_all = 0
        
        print(f"GitHub's built-in API reports {total_commits_all} total commits across all contributors")
        
        for contributor in contributors_stats:
            weeks = contributor.get("weeks", [])
            for week in weeks:
                total_additions_all += week.get("a", 0)
                total_deletions_all += week.get("d", 0)
        
        total_changes_all = total_additions_all + total_deletions_all
        
        # Calculate user totals
        user_total_commits = user_stats.get("total", 0)
        user_total_additions = 0
        user_total_deletions = 0
        
        weeks = user_stats.get("weeks", [])
        for week in weeks:
            user_total_additions += week.get("a", 0)
            user_total_deletions += week.get("d", 0)
        
        user_total_changes = user_total_additions + user_total_deletions
        
        # Check if this is a large repository (>10k commits) where GitHub doesn't provide line change data
        large_repo_warning = ""
        if total_changes_all == 0 and total_commits_all > 1000:
            large_repo_warning = " (GitHub doesn't provide line change data for repos with >10k commits)"
        
        # Calculate percentages
        user_commit_percentage = (user_total_commits / total_commits_all * 100) if total_commits_all > 0 else 0
        user_changes_percentage = (user_total_changes / total_changes_all * 100) if total_changes_all > 0 else 0
        
        return {
            "repository": f"{owner}/{repo}",
            "username": username,
            "total_commits": total_commits_all,
            "user_commits": user_total_commits,
            "user_commit_percentage": round(user_commit_percentage, 2),
            "total_repo_changes": total_changes_all,
            "total_user_changes": user_total_changes,
            "user_changes_percentage": round(user_changes_percentage, 2),
            "breakdown": {
                "user_additions": user_total_additions,
                "user_deletions": user_total_deletions,
                "repo_additions": total_additions_all,
                "repo_deletions": total_deletions_all
            },
            "contributor_count": len(contributors_stats),
            "data_source": "GitHub built-in stats API",
            "large_repo_warning": large_repo_warning
        }

async def main_async():
    # Get API token from environment
    api_token = os.getenv('GH_PROXY_API_KEY')
    if not api_token:
        print("Error: GH_PROXY_API_KEY environment variable not set")
        print("Please set it in your .env file or export it")
        sys.exit(1)
    
    # Get inputs from command line arguments or prompt
    if len(sys.argv) >= 3:
        repo_url = sys.argv[1]
        username = sys.argv[2]
        manual = len(sys.argv) >= 4 and sys.argv[3] == "--manual"
    else:
        repo_url = input("Enter GitHub repository URL: ").strip()
        username = input("Enter GitHub username: ").strip()
        manual = input("Force manual calculation? (y/N): ").strip().lower() in ['y', 'yes']
    
    if not repo_url or not username:
        print("Error: Both repository URL and username are required")
        sys.exit(1)
    
    try:
        stats_calculator = GitHubCommitStatsV2(api_token)
        owner, repo = stats_calculator.extract_repo_info(repo_url)
        
        # Try built-in API first, fall back to manual if needed
        if not manual:
            try:
                results = await stats_calculator.calculate_user_stats(repo_url, username)
                # If line changes are 0 and it's a large repo, automatically use manual calculation
                if results["total_repo_changes"] == 0 and results["total_commits"] > 1000:
                    print(f"\nBuilt-in API doesn't provide line changes for large repos (>10k commits)")
                    print("Automatically switching to manual calculation for accurate line change data...")
                    manual = True
            except Exception as e:
                print(f"Built-in API failed: {e}")
                manual = True
        
        if manual:
            print("Using manual calculation with individual commit API calls...")
            results = await stats_calculator.calculate_line_changes_manually(owner, repo, username)
        
        # Display results
        print("\n" + "="*60)
        print("GITHUB COMMIT STATISTICS (v2 - Enhanced)")
        print("="*60)
        print(f"Repository: {results['repository']}")
        print(f"Username: {results['username']}")
        if "contributor_count" in results:
            print(f"Total contributors: {results['contributor_count']}")
        print()
        print("COMMIT STATISTICS:")
        print(f"  Total commits in repo: {results['total_commits']:,}")
        print(f"  Commits by user: {results['user_commits']:,}")
        print(f"  User commit percentage: {results['user_commit_percentage']}%")
        print()
        print("LINE CHANGE STATISTICS:")
        print(f"  Total repository changes: {results['total_repo_changes']:,}")
        print(f"  Total user changes: {results['total_user_changes']:,}")
        print(f"  User changes percentage: {results['user_changes_percentage']}%{results.get('large_repo_warning', '')}")
        print()
        print("DETAILED BREAKDOWN:")
        print(f"  User additions: {results['breakdown']['user_additions']:,}")
        print(f"  User deletions: {results['breakdown']['user_deletions']:,}")
        print(f"  Repo additions: {results['breakdown']['repo_additions']:,}")
        print(f"  Repo deletions: {results['breakdown']['repo_deletions']:,}")
        print()
        print(f"Data source: {results['data_source']}")
        print("="*60)
        
        # Save results to JSON file
        output_file = f"commit_stats_v2_{results['repository'].replace('/', '_')}_{username}.json"
        with open(output_file, 'w') as f:
            json.dump(results, f, indent=2)
        print(f"\nResults saved to: {output_file}")
        
    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)

def main():
    asyncio.run(main_async())

if __name__ == "__main__":
    main()
