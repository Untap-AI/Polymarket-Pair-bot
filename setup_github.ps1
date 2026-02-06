# PowerShell script to set up and push to GitHub
# Run this AFTER you've installed Git and created a GitHub repo

Write-Host "Setting up Git repository..." -ForegroundColor Green

# Initialize git if needed
if (-not (Test-Path .git)) {
    git init
    Write-Host "Git repository initialized" -ForegroundColor Green
}

# Add all files
git add .
Write-Host "Files staged" -ForegroundColor Green

# Create initial commit
git commit -m "Initial commit: Polymarket pair measurement bot with 5 new measurement features"
Write-Host "Initial commit created" -ForegroundColor Green

# Instructions for creating GitHub repo
Write-Host "`n=== NEXT STEPS ===" -ForegroundColor Yellow
Write-Host "1. Go to https://github.com/new" -ForegroundColor Cyan
Write-Host "2. Create a new repository (e.g., 'polymarket-pair-bot')" -ForegroundColor Cyan
Write-Host "3. DO NOT initialize with README, .gitignore, or license" -ForegroundColor Cyan
Write-Host "4. Copy the repository URL" -ForegroundColor Cyan
Write-Host "5. Run these commands:" -ForegroundColor Cyan
Write-Host "   git remote add origin https://github.com/YOUR_USERNAME/YOUR_REPO_NAME.git" -ForegroundColor White
Write-Host "   git branch -M main" -ForegroundColor White
Write-Host "   git push -u origin main" -ForegroundColor White

Write-Host "`nOr, if you have GitHub CLI installed, run:" -ForegroundColor Yellow
Write-Host "   gh repo create polymarket-pair-bot --public --source=. --remote=origin --push" -ForegroundColor White
