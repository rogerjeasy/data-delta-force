# Data Delta Force - Macro-Crypto Risk Intelligence Platform

## U.S. Macro Regimes and Crypto Correlation Dynamics: A Sentiment-Driven Data Lake and Warehouse Study

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Python 3.9+](https://img.shields.io/badge/python-3.9+-blue.svg)](https://www.python.org/downloads/)
[![Status: In Development](https://img.shields.io/badge/status-in%20development-orange.svg)]()

---

## 📋 Table of Contents

- [Project Overview](#project-overview)
<!-- - [Team Members](#team-members) -->
- [Background and Motivation](#background-and-motivation)
- [Research Questions](#research-questions)
- [Data Sources](#data-sources)
- [Project Architecture](#project-architecture)
- [Installation and Setup](#installation-and-setup)
- [Project Structure](#project-structure)
- [Development Workflow](#development-workflow)
- [Timeline and Milestones](#timeline-and-milestones)
- [Documentation](#documentation)
- [Contributing](#contributing)
- [License](#license)
- [Contact](#contact)

---

## 🎯 Project Overview

This project investigates the dynamic relationships between U.S. macroeconomic indicators and cryptocurrency markets, focusing on how these correlations shift across different market regimes. We develop an integrated data analytics platform combining Data Lake and Data Warehouse architectures to process real-time market data, sentiment indicators, and macroeconomic signals.

### Key Objectives

1. Develop an integrated data lake architecture processing 50,000+ daily API calls from macro and crypto sources
2. Create cross-asset correlation models with >80% accuracy for regime classification and breakdown prediction
3. Build real-time monitoring dashboards for portfolio risk attribution across traditional and digital assets
4. Establish sentiment transmission models quantifying spillover effects between macro news and crypto markets
5. Generate tactical allocation signals with >65% accuracy and measurable alpha generation

### Academic Context

- **Institution**: Lucerne University of Applied Sciences and Arts (HSLU)
- **School**: School of Business
- **Course**: Data Warehouse and Data Lake
- **Duration**: 5 months
- **Supervisors**: PD Dr. Luis Terán, José Mancera, Jhonny Pincay

<!-- --- -->

<!-- ## 👥 Team Members

| Name | Student ID | Email | Role |
|------|-----------|-------|------|
| **Roger Jeasy Bavibidila** | 21-739-537 | roger.bavibidila@stud.hslu.ch | Data Ingestion & API Integration |
| **Thilo Holstein** | 16-913-774 | thilo.holstein@stud.hslu.ch | Data Lake Architecture & Streaming |
| **Pablo Bonete Garcia** | 24-859-878 | pablo.bonetegarcia@stud.hslu.ch | Analytics & Modeling | -->

<!-- --- -->

## 🌟 Background and Motivation

### Topic Relevance

The global financial landscape is experiencing unprecedented convergence between traditional macro factors and emerging digital assets. Cryptocurrency markets, once considered isolated from traditional finance, now demonstrate significant correlations with equities, bonds, and FX during periods of macro stress. With institutional adoption of crypto assets accelerating and total crypto market capitalization exceeding $2 trillion, understanding these cross-asset dynamics has become critical for portfolio construction, risk management, and tactical allocation decisions.

### Current Industry Challenges

Financial institutions face several critical challenges:

- **Correlation Instability**: Crypto-traditional asset correlations range from -0.2 to +0.8 depending on market regime
- **Sentiment Integration**: Traditional macro models lack alternative data sources like social sentiment
- **Regime Identification**: Difficulty distinguishing when crypto acts as risk asset vs. inflation hedge
- **Cross-Asset Contagion**: Limited tools for monitoring sentiment and volatility transmission

---

## 🔍 Research Questions

### BQ1 - Portfolio Risk Attribution and Regime Detection
**Objective**: Quantify portfolio risk from crypto exposure under different U.S. macro regimes (rate hikes, inflation shocks, CPI changes, jobs data) and predict regime switches.

### BQ2 - Cross-Asset Sentiment Transmission Analysis
**Objective**: Measure and predict how sentiment spillovers between traditional macro news and crypto markets affect portfolio performance.

### BQ3 - Tactical Allocation Signal Generation
**Objective**: Generate actionable buy/sell/hedge signals based on sentiment-macro divergences and correlation breakdowns.

### BQ4 - Macro Nowcasting Enhancement Through Crypto Signals
**Objective**: Determine if crypto market signals improve traditional macro forecasting.

### BQ5 - Crisis Propagation and Contagion Monitoring
**Objective**: Early detection of systemic risk spreading between traditional and crypto markets.

---

## 📊 Data Sources

### Dynamic Data Sources (Real-Time)

#### 1. CoinGecko API
- **Data**: BTC/ETH/altcoin prices, trading volumes, market cap, social metrics
- **Update Frequency**: Real-time price feeds, hourly social sentiment
- **Rate Limits**: 50 calls/min (free), 500 calls/min (paid)
- **Documentation**: https://coingecko.com/en/api/documentation

#### 2. FRED API (Federal Reserve Economic Data)
- **Data**: Fed funds rate, CPI, PCE, employment, GDP, yield curves, money supply
- **Update Frequency**: Daily for market rates, monthly for economic indicators
- **Rate Limits**: 120 requests/min, 120,000 requests/day
- **Documentation**: https://fred.stlouisfed.org/docs/api/fred/

### Static Data Sources

- Historical FOMC Meeting Minutes
- Cross-Asset Historical Correlations
- Market Regime Classification Data
- Cryptocurrency Exchange Listings

---

## 🏗️ Project Architecture

### Technology Stack

**Data Ingestion & Processing**
- Apache Kafka (Real-time streaming)
- Apache Airflow (ETL orchestration)
- Python 3.9+ (Core language)

**Storage & Data Management**
- AWS S3 (Data Lake)
- PostgreSQL/Snowflake (Data Warehouse)
- InfluxDB (Time Series Database)
- AWS Glue (Metadata cataloging)

**Analytics & Modeling**
- Pandas, NumPy (Data manipulation)
- Scikit-learn, Statsmodels (Machine learning)
- SciPy (Statistical analysis)

**Visualization & Dashboards**
- Power BI (Structured reporting)
- Plotly/Dash (Interactive dashboards)
- Streamlit (Ad-hoc exploration)

**Infrastructure**
- Docker & Docker Compose
- AWS (Cloud infrastructure)
<!-- - Terraform (Infrastructure as Code) -->

---

## 🚀 Installation and Setup

### Prerequisites

- Python 3.9 or higher
- Git
- Docker and Docker Compose (optional but recommended)
- AWS Account (for cloud deployment)
- API Keys for CoinGecko and FRED

### Local Development Setup

1. **Clone the repository**
```bash
git clone https://github.com/rogerjeasy/data-delta-force.git
cd data-delta-force
```

2. **Create virtual environment**
```bash
# Windows
python -m venv venv
.\venv\Scripts\activate

# Mac/Linux
python3 -m venv venv
source venv/bin/activate
```

3. **Install dependencies**
```bash
pip install -r requirements.txt
```

4. **Configure environment variables**
```bash
# Create .env file
cp .env.example .env

# Add your API keys
# COINGECKO_API_KEY=your_key_here
# FRED_API_KEY=your_key_here
# AWS_ACCESS_KEY_ID=your_key_here
# AWS_SECRET_ACCESS_KEY=your_key_here
```

5. **Initialize databases**
```bash
python scripts/setup_databases.py
```

6. **Run initial data load**
```bash
python scripts/initial_data_load.py
```

### Docker Setup (Recommended)

```bash
# Build and start all services
docker-compose up -d

# Check service status
docker-compose ps

# View logs
docker-compose logs -f
```

---

## 📁 Project Structure

```
data-delta-force/
├── src/                    # Source code
│   ├── data_ingestion/     # API clients and data collection
│   ├── data_lake/          # Data lake implementation
│   ├── data_warehouse/     # Data warehouse ETL and schema
│   ├── processing/         # Streaming and batch processing
│   ├── analytics/          # Analysis and modeling
│   ├── visualization/      # Dashboards and reports
│   └── utils/              # Utility functions
├── notebooks/              # Jupyter notebooks for exploration
├── tests/                  # Unit and integration tests
├── docs/                   # Project documentation
├── config/                 # Configuration files
├── data/                   # Data directory (gitignored)
├── scripts/                # Utility scripts
├── airflow/                # Airflow DAGs
├── dashboards/             # Dashboard artifacts
├── models/                 # Trained models
├── reports/                # Generated reports
└── infrastructure/         # Infrastructure as code
```

For detailed structure explanation, see [docs/architecture/project_structure.md](docs/architecture/project_structure.md)

---

## 💻 Development Workflow

### Branch Strategy

- `main` - Production-ready code, protected branch
- `develop` - Integration branch for features
- `feature/*` - Individual feature branches
- `hotfix/*` - Emergency fixes

### Commit Convention

Follow conventional commits:
- `feat:` New features
- `fix:` Bug fixes
- `docs:` Documentation updates
- `refactor:` Code refactoring
- `test:` Test additions
- `chore:` Maintenance tasks

Example:
```bash
git commit -m "feat: add CoinGecko API rate limiter"
git commit -m "fix: resolve FRED API timeout issue"
git commit -m "docs: update installation instructions"
```

### Pull Request Process

1. Create feature branch from `develop`
2. Implement changes with tests
3. Update documentation if needed
4. Submit PR with clear description
5. Request review from at least one team member
6. Address review comments
7. Merge after approval

### Code Quality Standards

- Follow PEP 8 style guide
- Write docstrings for all functions/classes
- Maintain test coverage >80%
- Use type hints where applicable
- Run linters before committing:
```bash
black src/
flake8 src/
mypy src/
```

---

## 📅 Timeline and Milestones

| Milestone | Timeline | Status |
|-----------|----------|--------|
| Project Proposal Submission | Week 2 (Sept 2025) | ✅ Completed |
| Data Source Setup & Initial Ingestion | Weeks 3-4 (Oct 2025) | 🔄 In Progress |
| Data Lake Architecture Implementation | Weeks 5-7 (Oct 2025) | 📋 Planned |
| Mid-term Presentation | Week 8 (Nov 2025) | 📋 Planned |
| Data Warehouse Design & Population | Weeks 9-11 (Nov 2025) | 📋 Planned |
| Analytics & Modeling Development | Weeks 12-14 (Dec 2025) | 📋 Planned |
| Dashboard & Visualization Creation | Weeks 15-16 (Jan 2026) | 📋 Planned |
| Final Report Writing | Weeks 17-18 (Jan 2026) | 📋 Planned |
| Final Presentation & Submission | Week 19 (Feb 2026) | 📋 Planned |

---

## 📚 Documentation

### Available Documentation

- [System Architecture](docs/architecture/system_architecture.md)
- [Data Lake Design](docs/architecture/data_lake_design.md)
- [Data Warehouse Design](docs/architecture/data_warehouse_design.md)
- [API Integration Guide](docs/api_documentation/)
- [Analytics Methodology](docs/research/methodology.md)
- [Meeting Notes](docs/meeting_notes/)

### Final Report Structure

The final report will follow this structure:

1. **Introduction**
   - Project Overview and Objectives
   - Domain Context and Background
   - Research Questions and Hypotheses

2. **Literature Review**
   - Domain Industry Analysis
   - Data Analytics in Finance
   - Data Lake and Warehouse Architectures

3. **Data Lake Implementation**
   - Architecture Design
   - Data Source Integration
   - Schema Evolution Strategy
   - Data Quality and Governance

4. **Data Ingestion and Processing**
   - API Integration Strategies
   - Real-time vs Batch Processing
   - Error Handling and Recovery

5. **Data Warehouse Design**
   - Dimensional Modeling Approach
   - ETL Process Implementation
   - Performance Optimization

6. **Analytics and Modeling**
   - Exploratory Data Analysis
   - Domain-Specific Model Development
   - Cross-Platform Analysis
   - Model Validation and Testing

7. **Visualization and Dashboard**
   - Dashboard Design Principles
   - Real-time Monitoring System
   - Interactive Analytics Interface

8. **Results and Insights**
   - Research Question Answers
   - Business Intelligence Findings
   - Industry Implications

9. **Conclusions and Future Work**
   - Project Summary
   - Technical Achievements
   - Limitations and Future Enhancements

---

## 🤝 Contributing

### Team Members

All team members should:
1. Create feature branches from `develop`
2. Write clear commit messages
3. Add tests for new functionality
4. Update documentation
5. Request code reviews before merging

### External Contributors

While this is an academic project, we welcome feedback and suggestions:
1. Open an issue to discuss proposed changes
2. Fork the repository
3. Create a feature branch
4. Submit a pull request with detailed description

---

## 📄 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

---

## 📧 Contact

**Project Group**: Data Delta Force

**Team Members**:
- Roger Jeasy Bavibidila - roger.bavibidila@stud.hslu.ch
- Thilo Holstein - thilo.holstein@stud.hslu.ch
- Pablo Bonete Garcia - pablo.bonetegarcia@stud.hslu.ch

**Supervisors**:
- PD Dr. Luis Terán
- José Mancera
- Jhonny Pincay

**Institution**: Lucerne University of Applied Sciences and Arts (HSLU)  
**GitHub Repository**: https://github.com/rogerjeasy/data-delta-force

---

## 🙏 Acknowledgments

- HSLU School of Business for project supervision
- CoinGecko and FRED for API access
- Open-source community for tools and libraries

---

**Last Updated**: September 29, 2025  
**Project Status**: Active Development  
**Expected Completion**: February 2026