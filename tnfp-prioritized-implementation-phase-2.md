# TNFP Food Access Tool — Prioritized Implementation Phases

**Source:** *Group Notes on Belmont Food Access Tool.docx* (April 23, 2026)
**Prototype:** https://nfp-food-insecurity-map-ecru.vercel.app/
**Repo:** https://github.com/Belmont-Data-Collaborative/nfp-food-insecurity-map
**Status:** Draft for review with Tommy before moving to Subtask 2

---

## How to read this document

Every feedback item from the source notes appears in exactly one of three buckets: **UI/MAP**, **DATA**, or **OTHER**. Each item has a one-sentence summary for prioritization, the original attribution (so you can trace an ask back to whoever raised it), and a count of how many separate notes were consolidated into it.

**Contributor key:** Exec Cmte = Executive Committee · C.J. · RB · AT · Garden team · PS · DZ · NEXT = the "NEXT / DATA" consolidation section at the end of the source doc.

**Priority Tiers:** Tasks marked P1 should be accomplished this iteration, P2 next iteration, P3 are on the backlog, and P4 are out of scope/not considered.

**Effort Estimates:** Tasks marked S require minimal effort, M require moderate effort, and L require large amounts of effort

---

## 1. UI / MAP

*Interface, map behavior, visualization, default filters, zoom/scale, layer controls, legends, tooltips, popups.*

### Magnitude / need delta - P3 L
Visualize the gap between need and available resources instead of a binary present/absent signal.
*Sources: Exec Cmte ("magnitude of the lack of access / need delta"); DZ ("magnitude of food access delivered by non-profit orgs"); DZ ("shade the districts… not just the binary"); NEXT ("delta between the need and the resources offered"). — 4 notes*

### Zoom / dot clustering - P1 S
Fix the visualization so users don't have to zoom in so far to separate clustered dots.
*Sources: Exec Cmte ("adjust the zoom in scale on the dots"); DZ ("alter the data visualization to avoid… zoom so far in to avoid clusters"). — 2 notes*

### Zip code / county views - P2 M
Add zip-code and county-level views, or overlay zip codes on the census tracts.
*Sources: Exec Cmte ("zip code and county level views"); DZ ("zip codes instead of census tracts or at least overlay"). — 2 notes*

### Define the "Food Access" score - P1 S
Explain what the tract-level food access score measures and how to interpret it.
*Sources: Exec Cmte ("low access on tract level: what is it measuring?"); DZ ("food access score… what is that?"); Garden team ("Food access number… what does this number mean?"). — 3 notes*

### LILA as default layer - P1 S
Decide whether LILA (not median income) should be the default layer to match expansion strategy.
*Sources: Exec Cmte ("Default filter LILA oriented?"); DZ ("LILA the default data layer rather than median income"). — 2 notes*

### Label fix: "TNFP" not "NFP" - P1 S
Relabel the partner button from "NFP" to "TNFP."
*Source: C.J. ("It's 'TNFP' not 'NFP'"). — 1 note*

### "Other" category - P2 M
Clarify what's inside the oversized "Other" category and whether it can be broken down further.
*Source: DZ ("'Other' is a huge category… better way to break it down?"). — 1 note*

### Org categories as food-insecurity proxy - P3 M
Question whether the non-meal, non-TNFP organization categories are valid proxies for food insecurity.
*Source: DZ ("are the other categories of organizations really a good proxy…"). — 1 note*

### LILA Designation vs. Low Income/Low Access Population filters - P2 S
Clarify how the LILA Designation filter differs from the Low Income/Low Access Population filter.
*Source: DZ (parenthetical in the food-access-score note). — 1 note*

### Sticky tract highlights - P1 S
When moving the mouse over the tracts, random tracts stay highlighted until the user mouses over them again.
*Source: added during triage review. — 1 note*

---

## 2. DATA

*New data sources, data updates/freshness, classification rubrics, derived metrics, and AI/conversational features over the data.*

### Add food access points (gardens, markets, grocery, fridges, stands, ethnic markets) - P1 L
Add every food-access-point type and distinguish food access from food production.
*Sources: Exec Cmte ("add… any food access point… differentiate food access and food production"); C.J. ("Grocery store data?"); Garden team ("Gardens are missing!"); DZ ("Grocery stores, gardens, community pantries, farmers markets"); NEXT ("Add grocery stores, gardens, community pantries, farmers markets — CJ will send sources"). — 6 notes*

### Add food pantry / food bank data - P1 M
Add Second Harvest (SHFB) and other food pantry / food bank sites.
*Sources: C.J. ("Need food pantry sites, both Second Harvest partners and others"); C.J. ("Get food pantries from SHFB"); NEXT ("Add in more food bank data"). — 3 notes*

### Distinguish TNFP vs. non-TNFP community meal sites - P2 L
Separate TNFP community meal partners from non-TNFP meal sites in the data.
*Source: Exec Cmte ("distinguish between TNFP community meal partners and other, non-TNFP community meal sites"). — 1 note*

### Update the LILA / USDA data - P1 M
Replace the outdated 2019 pre-pandemic LILA/USDA data with something more current.
*Sources: Exec Cmte ("The LILA data is old"); PS ("obtain more updated data — even if it's not from USDA… 2019 pre-pandemic snapshot"). — 2 notes*

### Add heart disease / CAD prevalence - P2 L
Add CAD / heart disease prevalence as a health indicator, with source metadata. (See MI hospitalizations below.)
*Sources: Exec Cmte ("Add heart disease prevalence — source link metadata?"); NEXT ("Add CAD prevalence as an indicator"). — 2 notes*

### Source / map MI hospitalizations - P3 L
Source and map myocardial infarction hospitalization data from healthdata.tn.gov.
*Source: PS ("Source and map myocardial infarction (MI) hospitalizations" + healthdata.tn.gov link). — 1 note*

### Add data-year labeling - P1 S
Label each layer with its data year so users can judge how current it is.
*Source: PS ("Input the data year(s) in the legend box or data layer list… aware of the data's age per layer"). — 1 note*

### TNFP hub sites and CAN network partners - P1 M
Add TNFP hub sites and CAN network partners to the map.
*Source: Garden team ("TNFP hub sites, and CAN network partners"). — 1 note*

### Other unaffiliated community ag work - P3 L
Add unaffiliated community-agriculture sites such as Cult2vate and Giving Garden.
*Source: Garden team ("Other unaffiliated community ag work… e.g. Cult2vate, Giving Garden"). — 1 note*

### Partner data clean-up - P1 M
Clean up partner records to match TNFP's partner classification rubric.
*Source: NEXT ("Partner data 'clean up' based on how TNFP classifies a partner"). — 1 note*

### Public transit data - P3 L
Add WeGo transit stops and routes.
*Source: NEXT ("Public Transit data — WeGo stops, routes?"). — 1 note*

### Identify the gaps - P2 L
Determine where the food-access gaps are across Nashville.
*Source: Exec Cmte ("Where are the gaps in Nashville?"). — 1 note*

### PO Box data issue - P1 M
Fix PO Box-based addresses that wrongly cluster data downtown.
*Source: Exec Cmte ("A lot of data is PO Box driven, i.e., downtown"). — 1 note*

### Potential additional source: TNCOMMGARD Mutual Aid Map - P3 M
Evaluate the TNCOMMGARD Mutual Aid Map as a candidate source (accuracy unverified).
*Source: AT ("Another potential data source… TNCOMMGARD Mutual Aid Map"). — 1 note*

### Open Table data - P4 M
Incorporate Open Table data once available.
*Source: C.J. ("Open Table data"). — 1 note*

### Add conversational AI - P4 L
Add a conversational AI so users can ask plain-language questions about the data.
*Source: NEXT ("Can conversational AI be added… ask it questions about the data," plus example prompts: "What resources are available to help feed my young family in 37208?" and "TNFP has $100k to invest, where would we make the most impact?"). — 1 note*

---

## 3. OTHER

*Training, partnership, maintenance/governance, contact processes, and anything that doesn't fit the first two buckets.*

### Define "partners" and the update/maintenance plan - P1 M
Define what counts as a "partner" and set a plan for keeping the data updated and maintained.
*Sources: AT ("how the group defines 'partners'"); AT ("plan to keep this updated… who/how to contact the creators"); Garden team ("What's the plan for maintaining this?"). — 3 notes*

### Offer training / working session on the tool - P2 S
Offer staff and possibly public training or working sessions once the tool is done.
*Sources: AT ("optional staff training"); AT ("community/public training… virtual and/or recorded"); Garden team ("a session to dig into this with the creators"); Garden team ("share w/ other McGruder partner orgs"). — 4 notes*

### Big-picture purpose - P1 S
Define what decisions the map should inform and what data best supports them.
*Source: DZ ("Big picture — what decisions do we want to use the map to inform…"). — 1 note*

### Define "community kitchen" - P2 M
Define what a "community kitchen" is within the Partner Classification Rubric.
*Source: RB ("What defines a community kitchen? What is it?"). — 1 note*

### Heart disease rationale *(context)* - P4 S
Context note motivating the CAD/MI data work: heart disease is TN's #1 killer (~25%), and diet matters especially post-heart-attack.
*Source: PS ("Heart disease is the #1 killer in TN — at a rate of 25%…"). — 1 note (context)*

### CHIP indicators - P3 M
Assess whether the CHIP indicators are useful.
*Source: C.J. ("To what extent are these CHIP indicators useful?"). — 1 note*

### Incorporate adjacent maps? - P3 M
Decide whether to incorporate existing similar/adjacent maps (accuracy unverified).
*Source: AT ("interested in incorporating existing maps similar/adjacent to this?"). — 1 note*

### Inform partner selection criteria - P2 M
Use the map to inform partner application selection criteria for orchards and gardens.
*Source: Garden team ("inform our selection criteria for partner applications — orchards and gardens"). — 1 note*

### Back-end data ownership - P1 S
Clarify who owns and holds the back-end data.
*Source: Garden team ("Curious: who holds the back-end data?"). — 1 note*

### Correlations *(note)* - P4 S
Observation that the correlations surfaced so far are striking.
*Source: Garden team ("Correlations are astounding"). — 1 note (note)*
