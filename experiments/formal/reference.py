#!/usr/bin/env python3
"""Exact local constant-floor versus richer-calendar reference.

This is exhaustive finite integer optimization for one admission snapshot,
not a global optimum for online GAR. The richer interface can atomically
preinstall several floor changes. Both families preserve the same delayed
prefix, immutable bookings, enforced future envelopes and constant tail.
No future realized packet arrivals are supplied to either family.
"""
from dataclasses import asdict, dataclass
import argparse
import csv
import hashlib
import itertools
import json
from math import prod
from pathlib import Path
import random
import time


@dataclass(frozen=True)
class Envelope:
    deadline: int
    sigma: int
    rho: int


@dataclass(frozen=True)
class Case:
    name: str
    capacity: int
    units: int
    base: int
    maximum: int
    background: int
    delay: int
    term: int
    tail: int
    prefix: tuple
    bookings: tuple
    pending: tuple  # (residual deadline boundary, remaining bytes), released now
    envelopes: tuple

    @property
    def end(self):
        return self.delay + self.term

    @property
    def horizon(self):
        return max(self.end + max((e.deadline for e in self.envelopes), default=0) + 1,
                   max((d for d, _ in self.pending), default=0), len(self.bookings))

    def validate(self):
        integers = [self.capacity, self.units, self.base, self.maximum, self.background,
                    self.delay, self.term, self.tail, *self.prefix, *self.bookings,
                    *(v for p in self.pending for v in p),
                    *(v for e in self.envelopes for v in (e.deadline, e.sigma, e.rho))]
        if any(type(x) is not int for x in integers):
            raise ValueError('Only exact integer model inputs are accepted')
        if self.capacity < 1 or self.units < 1 or not 0 <= self.base <= self.tail <= self.maximum <= self.units:
            raise ValueError('Invalid service or floor limits')
        if self.background < 0 or self.background >= self.units or self.delay < 0 or self.term < 1:
            raise ValueError('Invalid background or timing')
        if len(self.prefix) != self.delay or any(not self.base <= x <= self.maximum for x in self.prefix):
            raise ValueError('The entire immutable preactivation prefix is required')
        if any(x < 0 for x in self.bookings) or any(d <= 0 or work < 0 for d, work in self.pending):
            raise ValueError('Invalid existing bookings or pending obligations')
        if any(e.deadline <= 0 or e.sigma < 0 or e.rho < 0 for e in self.envelopes):
            raise ValueError('Invalid future envelope')


def demand(case, a, b):
    """Direct dense integer-interval formula; independent of C++ deque code."""
    value = sum(work for deadline, work in case.pending if a == 0 and deadline <= b)
    for e in case.envelopes:
        releases = max(0, b - e.deadline - max(a, 1) + 1)
        if releases:
            value += e.sigma + e.rho * releases
    return value


def tail_feasible(case):
    service = case.capacity * case.tail // case.units
    if sum(e.rho for e in case.envelopes) > service:
        return False
    # For positive starts, demand is piecewise affine in interval length.
    # Nonpositive asymptotic slope plus every deadline breakpoint suffices.
    for length in sorted({e.deadline for e in case.envelopes}):
        required = sum(e.sigma + e.rho * (length - e.deadline + 1)
                       for e in case.envelopes if e.deadline <= length)
        if required > service * length:
            return False
    return True


def intervals(case):
    return [(a, b, demand(case, a, b)) for b in range(1, case.horizon + 1) for a in range(b)]


def certified(case, pulse, checks=None, extend=0):
    if len(pulse) != case.term or not tail_feasible(case):
        return False
    floors = (*case.prefix, *pulse, *((case.tail,) * (case.horizon - case.end + extend)))
    prefix = [0]
    for u in floors:
        prefix.append(prefix[-1] + case.capacity * u // case.units)
    dense = checks if checks is not None and not extend else [
        (a, b, demand(case, a, b)) for b in range(1, len(floors) + 1) for a in range(b)]
    return all(prefix[b] - prefix[a] >= work for a, b, work in dense)


def caps(case, request_units):
    """Resource constraints also retain bookings after the new request ends."""
    cap = []
    for n in range(case.horizon):
        occupied = case.background + (case.bookings[n] if n < len(case.bookings) else 0)
        if case.delay <= n < case.end:
            limit = min(case.maximum, case.units - occupied - request_units)
            if limit < case.base:
                return None
            cap.append(limit)
        else:
            required = case.prefix[n] if n < case.delay else case.tail
            if required + occupied > case.units:
                return None
    return tuple(cap)


def cost(case, pulse):
    return sum(u - case.base for u in (*case.prefix, *pulse))


def changes(case, pulse):
    # Count scheduled changes from the end of the preserved prefix to the tail.
    before = case.prefix[-1] if case.prefix else pulse[0]
    count = 0
    for u in (*pulse, case.tail):
        count += u != before
        before = u
    return count


def solve(case, max_profiles=200000):
    case.validate()
    dense = intervals(case)
    rows = []
    for q in range(case.units - case.background - case.base + 1):
        limits = caps(case, q)
        row = {'request_units': q, 'request_work': case.term * (case.capacity * q // case.units),
               'constant_feasible': False, 'calendar_feasible': False,
               'constant_min_cost': None, 'calendar_min_cost': None,
               'constant_witness': None, 'calendar_witness': None, 'profiles_enumerated': 0}
        if limits is not None and tail_feasible(case):
            options = [range(case.base, u + 1) for u in limits]
            states = prod(map(len, options))
            if states > max_profiles:
                raise ValueError(f'{case.name}: {states} profiles exceed explicit exhaustive budget; no approximate result returned')
            feasible_constant = [u for u in range(case.base, min(limits) + 1)
                                 if certified(case, (u,) * case.term, dense)]
            if feasible_constant:
                witness = (min(feasible_constant),) * case.term
                row.update(constant_feasible=True, constant_min_cost=cost(case, witness), constant_witness=witness)
            # Enumerate every resource-feasible profile; minimize integral
            # floor area. Lexicographic ties make the witness deterministic.
            for pulse in itertools.product(*options):
                row['profiles_enumerated'] += 1
                if certified(case, pulse, dense):
                    rank = (cost(case, pulse), pulse)
                    if not row['calendar_feasible'] or rank < (row['calendar_min_cost'], row['calendar_witness']):
                        row.update(calendar_feasible=True, calendar_min_cost=rank[0], calendar_witness=pulse)
            # Independent monotonicity check: all demand inequalities increase
            # with each service slot, so the maximal legal profile decides
            # feasibility of the unrestricted per-slot family exactly.
            upper_feasible = certified(case, limits, dense)
            if upper_feasible != row['calendar_feasible']:
                raise AssertionError('Exhaustive calendar feasibility disagrees with monotonic upper profile')
            if row['constant_feasible'] and (not row['calendar_feasible'] or row['calendar_min_cost'] > row['constant_min_cost']):
                raise AssertionError('Constant family is not a subset of richer family')
            for family in ['constant', 'calendar']:
                if row[family + '_feasible']:
                    witness = row[family + '_witness']
                    row[family + '_changes'] = changes(case, witness)
                    if not certified(case, witness, extend=2 * max((e.deadline for e in case.envelopes), default=1) + 3):
                        raise AssertionError('Finite-plus-tail witness failed explicit longer-horizon check')
        rows.append(row)
    max_constant = max((r['request_units'] for r in rows if r['constant_feasible']), default=-1)
    max_calendar = max((r['request_units'] for r in rows if r['calendar_feasible']), default=-1)
    return {'case': asdict(case), 'horizon': case.horizon, 'tail_feasible': tail_feasible(case),
            'constant_max_request_units': max_constant, 'calendar_max_request_units': max_calendar,
            'constant_max_work': None if max_constant < 0 else case.term * (case.capacity * max_constant // case.units),
            'calendar_max_work': None if max_calendar < 0 else case.term * (case.capacity * max_calendar // case.units),
            'constant_zero_request_feasible': rows[0]['constant_feasible'],
            'calendar_zero_request_feasible': rows[0]['calendar_feasible'], 'request_levels': rows,
            'profiles_enumerated': sum(r['profiles_enumerated'] for r in rows)}


def benchmark_cases():
    """Declared mathematical fixtures, not sampled production operating states."""
    envelope = (Envelope(3, 2, 1),)
    cases = [Case('booking_valley', 8, 8, 0, 6, 2, 1, 3, 1, (2,), (0, 0, 3, 0), ((4, 6),), envelope),
             Case('flat_calendar', 8, 8, 0, 6, 2, 1, 3, 1, (2,), (0, 0, 0, 0), ((4, 6),), envelope),
             Case('early_urgent_pending', 8, 8, 0, 6, 2, 1, 3, 1, (2,), (0, 0, 3, 0), ((2, 5),), envelope),
             Case('immutable_prefix_failure', 8, 8, 0, 6, 2, 1, 3, 1, (1,), (0, 0, 3, 0), ((1, 2),), envelope),
             Case('booking_after_request', 8, 8, 0, 6, 2, 1, 3, 1, (2,), (0, 0, 3, 0, 6), ((4, 6),), envelope),
             Case('integer_service_rounding', 11, 8, 0, 6, 2, 2, 3, 1, (3, 1), (0, 0, 0, 3, 0), ((5, 8),), envelope)]
    rng = random.Random(230923)
    for index in range(24):
        delay, term = rng.choice([1, 2]), rng.choice([2, 3, 4])
        bookings = (0,) * delay + tuple(rng.randrange(4) for _ in range(term))
        cases.append(Case(f'declared_random_snapshot_{index:02d}', rng.choice([8, 11]), 8, 0, 6, 2,
                          delay, term, 1, tuple(rng.randrange(1, 4) for _ in range(delay)), bookings,
                          ((delay + term, rng.randrange(2, 11)),), envelope))
    return cases


def main(output):
    if output.exists():
        raise FileExistsError(f"Refusing to overwrite output directory: {output}")
    start = time.perf_counter()
    cases = benchmark_cases()
    results = [solve(case) for case in cases]
    output.mkdir(parents=True, exist_ok=False)
    (output / 'instances.json').write_text(json.dumps([asdict(c) for c in cases], indent=2) + '\n')
    (output / 'results.json').write_text(json.dumps(results, indent=2) + '\n')
    fields = ['name', 'horizon', 'constant_max_request_units', 'calendar_max_request_units',
              'constant_max_work', 'calendar_max_work', 'constant_zero_request_feasible',
              'calendar_zero_request_feasible', 'profiles_enumerated']
    with (output / 'summary.csv').open('w', newline='') as stream:
        writer = csv.DictWriter(stream, fieldnames=fields)
        writer.writeheader()
        for r in results:
            writer.writerow({'name': r['case']['name'], **{k: r[k] for k in fields if k != 'name'}})
    valid = [r for r in results if r['constant_zero_request_feasible']]
    report = {'status': 'PASS', 'cases': len(results), 'valid_constant_baseline_cases': len(valid),
              'constant_baseline_infeasible_cases_retained': len(results) - len(valid),
              'richer_admission_cases_among_valid_baselines': sum(r['calendar_max_request_units'] > r['constant_max_request_units'] for r in valid),
              'profiles_enumerated': sum(r['profiles_enumerated'] for r in results),
              'elapsed_seconds': time.perf_counter() - start,
              'source_sha256': hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
              'scope': 'Exact local certificate-family reference; one request per snapshot, integer floors, immutable delayed prefix and bookings, identical sustained tail. Rich family requires an atomically preloaded multi-change executor; no global online or production-admission optimality claim.',
              'sampling': 'Six declared mathematical fixtures and24 fixed-seed synthetic snapshots; illustrative mechanism benchmark, not a population estimate.'}
    (output / 'receipt.json').write_text(json.dumps(report, indent=2) + '\n')
    print(json.dumps(report, indent=2))


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output', type=Path, required=True)
    main(parser.parse_args().output)
