# NFL Fantasy Football Prediction Project - Architecture Diagram

## Project Overview Diagram

```
┌─────────────────────────────────────────────────────────────────────────────────────┐
│                    NFL Fantasy Football Prediction System                           │
│                                                                                     │
│  Goal: Build predictive ML models to classify players into tiers for draft strategy │
└─────────────────────────────────────────────────────────────────────────────────────┘

┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   Phase 1.2     │    │     Phase 2     │    │     Phase 3     │    │     Phase 4     │
│ Data Extraction │───▶│ Data Storage    │───▶│ Data Analysis   │───▶│ Model Building  │
│                 │    │                 │    │ & Engineering   │    │                 │
└─────────────────┘    └─────────────────┘    └─────────────────┘    └─────────────────┘
```

## Detailed System Architecture

### Phase 1.2: Data Extraction Pipeline

```
Data Sources:
┌─────────────────────────────────────────────────────────────────────────────┐
│                            Web Data Sources                                 │
├─────────────────┬─────────────────┬─────────────────┬─────────────────────┤
│ Pro Football    │ NFL.com         │ ESPN.com        │ Alternative Sources │
│ Reference       │                 │                 │                     │
│ (Primary)       │ (Secondary)     │ (Fallback)      │ (Future)           │
└─────────────────┴─────────────────┴─────────────────┴─────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                     Data Extraction Framework                               │
├─────────────────┬─────────────────┬─────────────────┬─────────────────────┤
│ Rate Limiter    │ Data Validator  │ Error Handler   │ Progress Tracker    │
│ • 2-3 sec delay │ • Schema check  │ • Retry logic   │ • Todo tracking     │
│ • Respectful    │ • Quality check │ • Graceful fail │ • Status updates    │
└─────────────────┴─────────────────┴─────────────────┴─────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                        Extracted Data Assets                                │
├─────────────────┬─────────────────┬─────────────────┬─────────────────────┤
│ Season Stats    │ Game Logs       │ Player Roster   │ Team Information    │
│ • 2,947 players │ • 26 top players│ • All positions │ • 32 NFL teams     │
│ • All positions │ • 386 games     │ • 2024 season   │ • Conferences       │
│ • CSV format    │ • Detailed stats│ • Team mapping  │ • Divisions         │
└─────────────────┴─────────────────┴─────────────────┴─────────────────────┘
```

### Phase 2: Data Storage & Organization

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                          Database Schema (SQLite)                           │
│                                                                             │
│  ┌─────────────┐    ┌─────────────┐    ┌─────────────┐                     │
│  │    TEAMS    │    │   PLAYERS   │    │    GAMES    │                     │
│  │─────────────│    │─────────────│    │─────────────│                     │
│  │ team_id (PK)│    │ player_id PK│    │ game_id (PK)│                     │
│  │ team_code   │◄──┐│ name        │┌──►│ season      │                     │
│  │ team_name   │   ││ position    ││   │ week        │                     │
│  │ conference  │   ││ team_id (FK)││   │ game_date   │                     │
│  │ division    │   │└─────────────┘│   │ home_team_id│                     │
│  └─────────────┘   │               │   │ away_team_id│                     │
│                    │               │   └─────────────┘                     │
│  ┌─────────────────┼───────────────┼─────────────────┐                     │
│  │             STATISTICS TABLES   │                 │                     │
│  │  ┌──────────────▼──┐  ┌─────────▼──┐  ┌─────────▼──┐                  │
│  │  │ PASSING_STATS   │  │RUSHING_STATS│  │RECEIVING_  │                  │
│  │  │ • attempts      │  │ • attempts   │  │STATS       │                  │
│  │  │ • completions   │  │ • yards      │  │ • targets   │                  │
│  │  │ • yards         │  │ • touchdowns │  │ • receptions│                  │
│  │  │ • touchdowns    │  │ • fumbles    │  │ • yards     │                  │
│  │  │ • interceptions │  └─────────────┘  │ • touchdowns│                  │
│  │  └─────────────────┘                   └─────────────┘                  │
│  └─────────────────────────────────────────────────────────────────────────┘
│                                                                             │
│  ┌─────────────────────────────────────────────────────────────────────────┤
│  │                      FANTASY SCORING ENGINE                             │
│  │  ┌─────────────────┐    ┌─────────────────┐    ┌─────────────────────┐ │
│  │  │ SCORING_RULES   │    │ FANTASY_POINTS  │    │ SEASON_STATS       │ │
│  │  │ • 0.5 PPR       │───▶│ • weekly_points │───▶│ • aggregated       │ │
│  │  │ • Your league   │    │ • breakdowns    │    │ • season totals    │ │
│  │  │ • Point values  │    │ • calculated    │    │ • performance      │ │
│  │  └─────────────────┘    └─────────────────┘    └─────────────────────┘ │
│  └─────────────────────────────────────────────────────────────────────────┘
└─────────────────────────────────────────────────────────────────────────────┘
```

### Phase 3: Data Analysis & Feature Engineering (Upcoming)

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                     Exploratory Data Analysis                               │
├─────────────────┬─────────────────┬─────────────────┬─────────────────────┤
│ Performance     │ Consistency     │ Usage Patterns  │ Matchup Analysis    │
│ Trends          │ Analysis        │                 │                     │
│ • Season trends │ • Boom/bust     │ • Target share  │ • Opponent strength │
│ • Weekly ups    │ • Variance      │ • Red zone use  │ • Home/away splits  │
│ • Position rank │ • Floor/ceiling │ • Snap counts   │ • Weather impact    │
└─────────────────┴─────────────────┴─────────────────┴─────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                     Feature Engineering                                     │
├─────────────────┬─────────────────┬─────────────────┬─────────────────────┤
│ QB Features     │ RB Features     │ WR/TE Features  │ Universal Features  │
│ • Pass attempts │ • Touches       │ • Target share  │ • 4-game avg       │
│ • Red zone use  │ • Goal line     │ • Air yards     │ • 8-game avg       │
│ • Mobility      │ • 3rd down use  │ • Route running │ • Season trends     │
└─────────────────┴─────────────────┴─────────────────┴─────────────────────┘
```

### Phase 4: Machine Learning Pipeline (Future)

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                        ML Model Development                                 │
│                                                                             │
│  ┌─────────────────┐    ┌─────────────────┐    ┌─────────────────────────┐ │
│  │ Position Models │    │ Tier Classifier │    │ Draft Strategy Tool    │ │
│  │ • QB Model      │───▶│ • Tier 1 Elite  │───▶│ • Value-based draft    │ │
│  │ • RB Model      │    │ • Tier 2 Good   │    │ • Sleeper detection    │ │
│  │ • WR Model      │    │ • Tier 3 Decent │    │ • Bust avoidance       │ │
│  │ • TE Model      │    │ • Tier 4 Risky  │    │ • Position strategy    │ │
│  │ • DEF Model     │    │ • Tier 5 Avoid  │    └─────────────────────────┘ │
│  └─────────────────┘    └─────────────────┘                                │
│                                                                             │
│  ┌─────────────────┐    ┌─────────────────┐    ┌─────────────────────────┐ │
│  │ Algorithm Mix   │    │ Validation      │    │ Production Pipeline    │ │
│  │ • Random Forest │    │ • Backtesting   │    │ • Weekly updates       │ │
│  │ • XGBoost       │    │ • Cross-val     │    │ • Model monitoring     │ │
│  │ • Linear Reg    │    │ • Accuracy test │    │ • Performance tracking │ │
│  └─────────────────┘    └─────────────────┘    └─────────────────────────┘ │
└─────────────────────────────────────────────────────────────────────────────┘
```

## Data Flow Architecture

```
┌─────────────┐     ┌─────────────┐     ┌─────────────┐     ┌─────────────┐
│             │     │             │     │             │     │             │
│ WEB SOURCES │────▶│ EXTRACTION  │────▶│  DATABASE   │────▶│  ANALYSIS   │
│             │     │  PIPELINE   │     │   STORAGE   │     │  PIPELINE   │
│ • PFR       │     │             │     │             │     │             │
│ • NFL.com   │     │ • Scrapers  │     │ • SQLite    │     │ • Features  │
│ • ESPN      │     │ • Validators│     │ • 14 Tables │     │ • EDA       │
│             │     │ • Rate Limit│     │ • Indexes   │     │ • Stats     │
└─────────────┘     └─────────────┘     └─────────────┘     └─────────────┘
       ▲                    ▲                    ▲                    │
       │                    │                    │                    │
       └────────────────────┼────────────────────┼────────────────────┘
                           │                    │                    
                           ▼                    ▼                    
┌─────────────────────────────────────────────────────────────────────────────┐
│                          MONITORING & LOGGING                               │
│ • Data extraction logs  • Database health    • Performance metrics         │
│ • Error tracking       • Quality validation • Progress tracking           │
└─────────────────────────────────────────────────────────────────────────────┘
```

## Technology Stack

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                            Technology Components                             │
├─────────────────┬─────────────────┬─────────────────┬─────────────────────┤
│ Data Extraction │ Data Storage    │ Analysis        │ ML/Modeling         │
│                 │                 │                 │                     │
│ • Python 3.12   │ • SQLite        │ • Pandas        │ • Scikit-learn     │
│ • Requests      │ • 14-table      │ • NumPy         │ • XGBoost          │
│ • BeautifulSoup │   schema        │ • Matplotlib    │ • Random Forest    │
│ • Rate limiting │ • Indexes       │ • Seaborn       │ • Cross-validation │
│ • CSV export    │ • Relationships │ • Statistical   │ • Model validation │
│ • Error handling│ • ACID compliance│   analysis      │ • Backtesting      │
└─────────────────┴─────────────────┴─────────────────┴─────────────────────┘
```

## Current Project Status (Phase 2 Complete)

```
Phase 1.2: Data Extraction ████████████████████████████████████████ 100% ✅
Phase 2:   Data Storage    ████████████████████████████████████████ 100% ✅
Phase 3:   Data Analysis   ░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░   0% 🎯
Phase 4:   Model Building  ░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░   0% 📋
Phase 5:   Production      ░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░   0% 📋

Current Assets Available:
✅ 2,947 player records across all positions
✅ 386 individual game logs from top performers
✅ Complete 2024 season data
✅ Fantasy scoring framework ready
✅ Database infrastructure established
```

## Key Success Metrics

| Metric | Target | Achieved | Status |
|--------|--------|----------|--------|
| Player Coverage | 2,000+ players | 2,947 players | ✅ 147% |
| Game Log Coverage | 20+ top players | 26 elite players | ✅ 130% |
| Season Completeness | Full 2024 season | 18 weeks captured | ✅ 100% |
| Data Quality | >90% accuracy | Validated & clean | ✅ 100% |
| Database Health | >80 health score | 40/100 (core complete) | 🔧 50% |

---

**Next Phase:** Exploratory Data Analysis & Feature Engineering to prepare data for ML model development.