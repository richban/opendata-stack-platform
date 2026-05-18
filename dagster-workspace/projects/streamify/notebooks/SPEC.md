# NBV Business Spec

**Project Specification: Multi-Entity Event-Sourced Transactional Ledger**

This specification defines the multi-entity architecture for our reinsurance volume performance engine. The system models user-submitted estimates and historical ledger facts as separate transactional structures, utilizing delta-journaling to reconcile projections.

#### **1. System Entities**

To prevent granularity conflation and preserve historical lineage, the architecture defines four distinct entities:

- **User Submission Estimates (Bronze)**: An append-only log of user-submitted planning snapshots. Each record contains the intended annual target (‭$T_A$‬) or monthly estimate (‭$T_M$‬) for a given account, year, and currency.
- **The Seed Ledger**: An external database containing historical actual transactions, cedent reported bookings, and retroactive adjustments.
- **The Transactional Ledger (Silver Journal)**: The master journal that contains the processed history of both actual bookings (imported from the Seed Ledger) and calculated estimates (translated from User Submissions).
- **The Control Ledger**: A log of account-specific holdback dates (‭$H_D$‬) representing the closed period boundary up to which all cash flows are verified.

#### 1.2 Principle of Intent by Granularity

The system treats the latest user submission snapshot as the authoritative truth, executing mutations based on the granularity of the user's explicit intent:

- **Global Annual Intent:** An Annual submission ($T_A$) establishes a new target for the entire calendar year. The latest snapshot completely supersedes all prior annual targets, triggering a redistribution and delta-rebalancing across all open months to match the new total period target.
- **Targeted Monthly Intent:** A Monthly submission ($T_M$) establishes an authoritative target strictly for the underwriting months explicitly present in the snapshot ($M_{\text{sub}}$). The system isolates the delta calculation to these specified periods. Unmentioned months are treated as outside the scope of the snapshot's intent, and their historical ledger entries remain fully intact.

#### **2. Mathematical Rebalancing & Deltas**

Every state change in the Transactional Ledger is booked as a delta entry to ensure that the running sum always reflects the latest authoritative target.

**The Delta Rule for Estimates**

When a new estimate snapshot is processed, the target state for each underwriting month ‭$M$‬ is computed first. The delta to be booked in the journal is the target state minus the running sum of all estimate entries already recorded:

$$\text{Delta}_M = \text{Target}_M - \sum \text{ExistingEstimates}_M$$

Where the monthly target state is derived based on the active mode:

**Monthly Granularity Mode ("M")**

The target for month ‭$M$‬ is directly taken from the user estimate:

$$\text{Target}_M = T_{M}$$

**Annual Granularity Mode ("A")**

The annual target ‭$T_A$‬ is redistributed from the current holdback date (‭$H_D$‬) to the end of the calendar year across the remaining open months (‭$N$‬):

$$N = \sum_{M > H_D} 1$$

$$\text{Target}_M = \begin{cases} 0 & \text{if } M \le H_D \\ \frac{T_A}{N} & \text{if } M > H_D \end{cases}$$

**Reversal on Mode Switches ($A \leftrightarrow M$)**

When switching from Annual to Monthly mode, the ledger does not automatically clear or zero out months that are omitted from the new monthly submission. Previous months or months not explicitly included in the new monthly batch are left untouched.

The delta calculation is executed strictly for the specific underwriting months ‭$M_{\text{sub}}$‬ present in the new monthly submission:

$$\text{Delta}_{M_{\text{sub}}} = T_{M_{\text{sub}}} - \sum \text{ExistingEstimates}_{M_{\text{sub}}}$$

Any month ‭$M$‬ not included in the new monthly submission is ignored by the reconciliation run, and its historical ledger entries remain fully intact. This ensures that the system only mutates the states of months the user explicitly intends to adjust.

**3. Operations & Ingestion Flow**

The system runs sequentially:

1. **Bootstrap Actuals**: Fetch seed data from the Seed Ledger and load it into the Silver Transactional Ledger as ACTUAL entries.
2. **Reconcile Estimates**: For any new Bronze Estimate, fetch the latest snapshot, calculate the monthly targets based on ‭$H_D$‬, compare them to existing entries, and book the deltas.
3. **Closing Window Moves**: Moving ‭$H_D$‬ forward triggers a reversal of estimates for newly closed months and prompts the re-distribution of any remaining annual estimates in the open window.

## Operational Windows and Boundary Protection

To preserve mathematical stability, the redistribution engine enforces a strict temporal boundary where the allocation window always initiates from the month immediately following the active holdback date ($H_D$).

### Current Period Redistribution Window

For an annual target $T_A$ within a calendar year, the remaining open months ($N$) available for distribution are calculated relative to the month index of $H_D$ (where January = 1, December = 12):

$$N = 12 - \text{Month}(H_D)$$

The target state for any underwriting month $M$ within the current financial year is governed by the following boundary rules:

$$\text{Target}_M = \begin{cases} 0 & \text{if } \text{Month}(M) \le \text{Month}(H_D) \\ \frac{T_A}{N} & \text{if } \text{Month}(M) > \text{Month}(H_D) \end{cases}$$

### Period Boundary Exception (December HBD Execution)

When the holdback date is advanced to the final month of the current financial period ($\text{Month}(H_D) = 12$):

- The remaining open months variable evaluates to zero ($N = 0$).
- The target state for all underwriting months within the current financial year evaluates to 0.
- The reconciliation engine automatically bypasses current-year redistribution calculations. The system requires that the subsequent financial period structure be initialized, shifting all forward-looking estimate distributions to the next annual planning cycle.

---

## 2.3 Temporal Mechanics for Retroactive Holdback Adjustments

The system supports bidirectional movement of the holdback date ($H_D$). When late-arriving actual bookings or retroactive adjustments force $H_D$ to move backward into a previously closed period, the system must recalculate the allocation matrix to incorporate the newly exposed timeline.

### Reopened Period Redistribution

Moving $H_D$ backward expands the open processing window, effectively turning previously locked periods back into open months. Upon a backward shift, the system executes a mandatory reallocation run:

- **Window Expansion:** The remaining open months count ($N$) is dynamically updated using the expanded range where $\text{Month}(M) > \text{Month}(H_D)_{\text{new}}$.
- **Retroactive Target Re-allocation:** The active annual estimate ($T_A$) is immediately redistributed across the newly expanded window, assigning a renewed target state ($\frac{T_A}{N}$) to both the newly reopened months and the remaining forward months.
- **Delta Balancing:** The reconciliation engine calculates the variance between the renewed monthly target state and the historical running sum for the reopened periods, booking the corresponding adjusting deltas into the ledger:

$$\text{Delta}_{M_{\text{reopened}}} = \left( \frac{T_A}{N_{\text{expanded}}} \right) - \sum \text{ExistingEstimates}_{M_{\text{reopened}}}$$

This guarantees that the running total of the ledger automatically re-balances to reflect the annual intent across all permitted periods without wiping out historical audit history.
