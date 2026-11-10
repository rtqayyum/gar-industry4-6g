# Conditional tail extension and transaction algorithm

This appendix is a draft for integration and independent mathematical review. It follows the discrete demand definition implemented in deadline_certificate.hpp. Its scope is a byte-service gateway with conforming arrival contracts, adequate storage, no post-ingress loss and the stipulated service supply. It does not prove a host scheduling bound or crash durability.

## Finite horizon and continuing tail

Let the service calendar become constant at boundary T, supplying c bytes per slot thereafter. Let R be the sum of the envelope rates and mmax the largest relative deadline. Pending work is already released at observation boundary zero. Check every demand interval ending no later than H, where H is at least T+mmax+1 and every pending deadline. Check the constant-tail rate and each distinct deadline breakpoint. Resource checks additionally cover every existing and proposed booking end.

For a positive interval start a, its future-demand term depends only on interval length ell. An envelope with deadline m contributes zero for ell below m and contributes sigma+rho(ell-m+1) at and above m. Between consecutive deadline breakpoints the slope of total demand minus c times length is the sum of the active rates minus c. That slope is nonpositive because every active rate sum is at most R and R is at most c. Each distinct breakpoint is checked after its demand jump. Before the first breakpoint demand is zero. These facts cover every interval wholly inside the constant tail, including repeated deadlines and zero rates.

For a zero-start interval ending after H, all pending work and all future-demand classes have become active by H. Extending the endpoint by one slot adds exactly R to the demand bound and exactly c to service. Slack therefore cannot decrease. Feasibility at H implies feasibility for every later endpoint.

For an interval starting before T at a positive boundary, every future-demand class is active by endpoint H because H is at least T+mmax+1. Moving its endpoint beyond H again increases bounded demand by R per slot and service by c per slot. Its checked slack at H cannot decrease. This covers every interval crossing the last calendar change.

Together the finite intervals, the wholly-tail intervals and the crossing intervals cover all integer intervals. The argument also covers T=0 and an empty pending set. It is a sufficient certificate; it is not a necessary schedulability test for arbitrary traffic.

## Preservation over transactions

Assume the installed calendar is certified and its complete booking ledger is legal. An unchanged-calendar admission preserves critical service and must fit every slot of its full requested interval. A replacement must retain the exact installed prefix until activation, including intervening old fallbacks. Its certificate must cover pending critical work, continuing bounded arrivals and its final constant tail. Its resource check must retain every older entitlement through that entitlement's end.

Commit the checked calendar and booking together only while the snapshot and state versions still match. Rejecting a proposal changes neither commitment. Activations and fallbacks execute the already checked calendar. Induction over serialized accepted transactions preserves the conditional critical-service and resource invariants. This does not assert that a new conservative certificate is obtainable at every later snapshot.

A controller interruption does not alter an already committed autonomous calendar. This statement requires the executor and promised payload availability to remain in operation. A gateway crash that interrupts service is a separate availability failure; a durable journal alone cannot repair deadlines already missed.

## Staged tenant availability

For an initially empty tenant queue, completion by its end boundary is equivalent, under exact available supply, to every suffix of released work fitting the remaining suffix of supply. Repeatedly expanding the queue recursion gives final backlog as the maximum positive suffix deficit. A release-aware suffix check therefore suffices under the stipulated lower supply bound. Necessity applies to exact available supply, not when unmodelled spare service might also be borrowed.

For robust release contracts, replace each actual suffix demand by a valid upper bound derived from the minimum cumulative release before that boundary. State how those release promises are enforced. Missing promised releases are contract violations and remain visible in offered-work results.

## Detailed transaction path

1. Apply every due installation and fallback in chronological order. A new installation supersedes a coincident old fallback.
2. Snapshot the current queues, complete projected calendar, future bookings, configuration and their versions.
3. Validate request fields and release availability. Run the optional causal headroom screen separately from the safety certificate.
4. Check whether the complete request fits the unchanged calendar and older bookings. If so, commit the booking against the current version.
5. Otherwise, search only the declared replacement family. Preserve the old activation prefix and check the complete continuing tail.
6. Check resource legality through all older and proposed booking ends. Check each accepted tenant's availability contract.
7. Independently recheck the candidate at the gateway. Reject stale snapshots, conflicting retries, expired commands and invalid configuration.
8. Commit the accepted calendar and booking atomically, then acknowledge the exact accepted identity. An identical retry returns the original acknowledgement without a second booking.
9. Execute activation, service quotas and fallback at their scheduled boundaries. Borrowed service remains revocable.
10. Record rejections and failures. Add an independently specified durable recovery protocol before making persistent gateway restart claims.

The implementation limits replacement while another command is pending. This is a search/interface limitation, not a proof that no other safe schedule exists. Command price only chooses among certified actions; it is not a monetary or energy measurement. State-construction and ledger-indexing cost must be stated in addition to the certificate search bound.
