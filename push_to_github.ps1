# Run this AFTER creating the repo on GitHub
# Replace YOUR_USERNAME with your GitHub username

$latest = Get-ChildItem "$env:LOCALAPPDATA\GitHubDesktop\app-*" -Directory | Sort-Object Name -Descending | Select-Object -First 1
$env:Path += ";$($latest.FullName)\resources\app\git\cmd"

Write-Host "Enter your GitHub username:" -ForegroundColor Yellow
$username = Read-Host

Write-Host "Enter your repository name (default: polymarket-pair-bot):" -ForegroundColor Yellow
$repoName = Read-Host
if ([string]::IsNullOrWhiteSpace($repoName)) { $repoName = "polymarket-pair-bot" }

git remote add origin "https://github.com/$username/$repoName.git"
git branch -M main
git push -u origin main

Write-Host "`nDone! Your repo is at: https://github.com/$username/$repoName" -ForegroundColor Green
