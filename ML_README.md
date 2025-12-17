# Loan Approval Prediction & Credit Risk Analysis

## Business Problem Summary

Financial institutions must balance profitability and risk when approving loans. Approving high-risk applicants can increase default losses, while rejecting creditworthy borrowers can reduce potential revenue and financial inclusion.

  This project aims to build a data-driven loan approval prediction model that assesses borrower credit risk using financial, credit, and behavioral variables. The objective is not only high predictive accuracy but also economic interpretability, ensuring that model decisions align with real-world lending logic.

## Dataset Overview

- **Observations:** 20,000 loan applicants  
- **Features:** 36 variables capturing borrower demographics, income, debt burden, credit history, and financial assets  
- **Target Variable:**  
  - `LoanApproved` (1 = Approved, 0 = Rejected)

### Key Feature Groups

- **Credit Risk Indicators:** CreditScore, RiskScore, PaymentHistory, BankruptcyHistory  
- **Affordability Measures:** Debt-to-Income Ratio, MonthlyLoanPayment, MonthlyDebtPayments  
- **Income & Wealth:** AnnualIncome, MonthlyIncome, NetWorth, TotalAssets  
- **Loan Characteristics:** LoanAmount, LoanDuration, InterestRate  

The dataset is moderately imbalanced, with approximately 24% approved loans and 76% rejections, reflecting realistic credit screening behavior.


## Approach & Methodology

The overall approach of this project was to first understand the borrower data, then identify meaningful risk patterns, and finally validate those patterns using predictive models. The analysis followed a structured progression from data understanding to interpretation, ensuring that each modeling choice was economically sensible and transparent.

  The first step involved developing a clear understanding of the dataset and its variables. Each feature was examined in terms of what it represents from a credit risk perspective includinh income, debt obligations, credit behavior, and financial buffers. Particular attention was paid to the target variable, LoanApproved, to assess class balance and confirm that the dataset reflects realistic lending outcomes. This stage helped establish which variables were likely to influence approval decisions and which might not hold much significance. The data was cleaned and validated before further analysis. Data types were carefully checked, and categorical variables were encoded using one-hot encoding so they could be used by classification algorithms. The application date was removed from the feature set because raw dates do not carry direct economic meaning in a non–time-series context and could introduce unnecessary noise or data leakage. This step ensured that all features used in the models were numerically meaningful and analytically defensible.

  Exploratory Data Analysis (EDA) was then conducted with a focus on economic interpretation rather than visual complexity. Instead of plotting variables in isolation, the analysis examined how key financial indicators differ between approved and rejected applicants. Credit scores, debt-to-income ratios, loan payment burdens, and income levels were analyzed to understand the underlying drivers of loan approval. The purpose of EDA was not just to visualize distributions, but to test whether observed patterns align with standard lending logic, such as the importance of repayment capacity and credit discipline.

### Modeling Strategy

Multiple classification models were trained to predict loan approval outcomes based on the insights of EDA. K-Nearest Neighbors (KNN) was included as a distance-based baseline model, requiring feature scaling to ensure fair distance calculations. Decision Tree and Random Forest models were then used to capture non-linear relationships and interaction effects commonly present in credit data. A train–test split with stratification was applied to preserve the approval–rejection ratio, and cross-validation was used on the training set to assess model stability and reduce the risk of overfitting.

  Model performance was evaluated using multiple metrics, rather than relying solely on accuracy. ROC-AUC was emphasized as the primary metric because it measures the model’s ability to distinguish between approved and rejected applicants across different thresholds. Precision, recall, F1-score, and confusion matrices were also examined to understand the trade-offs between approving risky borrowers and rejecting creditworthy ones. This multi-metric evaluation reflects real-world credit decision-making, where different types of errors carry other costs. The best-performing model, Random Forest, was analyzed using feature importance and partial dependence plots to understand which variables drive decisions and in which direction. This step ensured that the final model was not only accurate but also economically intuitive and explainable, reinforcing trust in the results and aligning the analysis with real-world credit risk assessment practices.


## Final Model Performance

| Model | CV ROC-AUC | Test ROC-AUC |
|------|-----------|--------------|
| KNN | 0.957 | 0.960 |
| Decision Tree | 0.988 | 0.991 |
| Random Forest | 0.999 | 0.999 |

The Random Forest model achieved the best and most stable performance across all metrics, with strong generalization on the test set.

## Key Insights

**Credit behavior is the strongest driver of loan approval decisions.**  
Variables such as `CreditScore`, `RiskScore`, and historical repayment indicators consistently emerged as the most influential features across exploratory analysis and model interpretation. Applicants with stronger credit histories were significantly more likely to receive loan approval. This indicates that the lending decision process is primarily backward-looking, placing high value on demonstrated financial reliability rather than speculative future capacity alone.

**Affordability plays a more decisive role than income alone.**  
While higher income levels are generally associated with better approval outcomes, income by itself does not guarantee loan approval. Applicants with relatively high earnings but heavy existing debt burdens were still likely to be rejected. This highlights that lenders prioritize net affordability over gross income figures.

**Debt-to-Income ratios act as a critical risk threshold.**  
Both exploratory analysis and partial dependence plots show a sharp decline in approval probability as debt-to-income ratios increase beyond moderate levels. This indicates that debt burden functions as a gating mechanism in credit decisions, where excessive leverage outweighs other positive borrower attributes such as income or asset ownership.

**Loan approval decisions are largely independent of demographic characteristics.**  
Demographic and background variables, including marital status and education level, exhibited relatively low importance in both model training and interpretability analysis. This suggests that approval decisions in the dataset are driven primarily by financial behavior and risk indicators rather than personal or demographic traits, which is desirable from both a risk management and fairness perspective.

**Overall, the observed patterns closely reflect real-world credit risk assessment practices.**  
The dominance of credit history, affordability, and debt burden aligns well with how financial institutions typically evaluate borrowers. This alignment increases confidence that the model is capturing economically meaningful relationships rather than spurious correlations, reinforcing its suitability for practical credit decision support.


## Final Model Choice & Justification

The Random Forest model was selected as the final model because it:

- Delivers superior predictive performance  
- Is robust to noise and overfitting  
- Learns economically meaningful patterns  
- Can be interpreted using feature importance and partial dependence analysis  

This makes it well-suited for supporting real-world loan approval decisions where both accuracy and explainability are important.

## How to Run the Project

### Requirements

- Python 3.9+  
- Libraries:
  - pandas  
  - numpy  
  - matplotlib  
  - seaborn  
  - scikit-learn  

### Steps

1. Clone the repository or download the project files  
2. Open the main Jupyter notebook in **Google Colab**  
3. From the top menu, click **Runtime → Run all**  
4. All cells will execute sequentially, covering:
   - Data loading & cleaning  
   - Exploratory Data Analysis (EDA)  
   - Modeling & evaluation  
   - Model interpretability analysis  
5. All results, tables, and visualizations will be generated automatically within the notebook

