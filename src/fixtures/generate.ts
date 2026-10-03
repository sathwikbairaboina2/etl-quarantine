export type BadKind =
  | 'date_dmy'
  | 'bad_email'
  | 'bad_currency'
  | 'bad_amount'
  | 'bad_plan'
  | 'missing_id'
  | 'bad_country'
  | 'extra_column';

export const BAD_KINDS: readonly BadKind[] = [
  'date_dmy',
  'bad_email',
  'bad_currency',
  'bad_amount',
  'bad_plan',
  'missing_id',
  'bad_country',
  'extra_column',
];

export const CUSTOMERS_HEADER = 'customer_id,email,country,currency,amount,contract_start,plan';

export function mulberry32(seed: number): () => number {
  let a = seed >>> 0;
  return () => {
    a = (a + 0x6d2b79f5) >>> 0;
    let t = a;
    t = Math.imul(t ^ (t >>> 15), t | 1);
    t ^= t + Math.imul(t ^ (t >>> 7), t | 61);
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
  };
}

export interface GenerateOptions {
  rows: number;
  badRate: number;
  seed: number;
  /** Receives every line (header first, no trailing newline). When set, no csv string is built. */
  onLine?: (line: string) => void;
}

export interface GenerateResult {
  csv?: string;
  labels: Map<number, BadKind>;
}

const COUNTRIES = ['DE', 'FR', 'NL', 'ES', 'IT', 'GB', 'US', 'SE', 'PL'];
const CURRENCIES = ['EUR', 'USD', 'GBP'];
const PLANS = ['basic', 'pro', 'enterprise'];

const pad2 = (n: number) => String(n).padStart(2, '0');

export function generateCustomers(opts: GenerateOptions): GenerateResult {
  const rnd = mulberry32(opts.seed);
  const pick = <T>(xs: readonly T[]): T => xs[Math.floor(rnd() * xs.length)]!;
  const labels = new Map<number, BadKind>();
  const out: string[] = [];
  const emit = (line: string) => (opts.onLine ? opts.onLine(line) : out.push(line));

  emit(CUSTOMERS_HEADER);
  for (let i = 1; i <= opts.rows; i++) {
    let id = `C-${i}`;
    let email: string = `user${i}@example.com`;
    let country = pick(COUNTRIES);
    let currency = pick(CURRENCIES);
    let amount = `${Math.floor(rnd() * 100000)}.${pad2(Math.floor(rnd() * 100))}`;
    let start = `${2020 + Math.floor(rnd() * 6)}-${pad2(1 + Math.floor(rnd() * 12))}-${pad2(1 + Math.floor(rnd() * 28))}`;
    let plan = pick(PLANS);
    let extra = false;

    // Messy-but-valid variants so normalisation is exercised.
    const m = rnd();
    if (m < 0.05) email = `  User${i}@Example.COM `;
    else if (m < 0.08) email = '';
    if (rnd() < 0.05) currency = currency.toLowerCase();
    if (rnd() < 0.05) plan = ` ${plan[0]!.toUpperCase()}${plan.slice(1)} `;
    const quoteEmail = rnd() < 0.02;

    if (rnd() < opts.badRate) {
      const kind = pick(BAD_KINDS);
      labels.set(i, kind);
      switch (kind) {
        case 'date_dmy':
          start = `${13 + Math.floor(rnd() * 16)}/${pad2(1 + Math.floor(rnd() * 12))}/${2020 + Math.floor(rnd() * 6)}`;
          break;
        case 'bad_email':
          email = `name${i}@`;
          break;
        case 'bad_currency':
          currency = 'EURO';
          break;
        case 'bad_amount':
          amount = `${Math.floor(rnd() * 1000)},${pad2(Math.floor(rnd() * 100))}`;
          break;
        case 'bad_plan':
          plan = 'gold';
          break;
        case 'missing_id':
          id = '';
          break;
        case 'bad_country':
          country = 'DEU';
          break;
        case 'extra_column':
          extra = true;
          break;
      }
    }

    const emailField = quoteEmail ? `"${email}"` : email;
    emit([id, emailField, country, currency, amount, start, plan].join(',') + (extra ? ',surprise' : ''));
  }
  return opts.onLine ? { labels } : { csv: `${out.join('\n')}\n`, labels };
}

/** Serialisable form of the labels, as stored next to the fixtures. */
export function labelsToJson(labels: Map<number, BadKind>): { badRows: number[]; kinds: Record<string, BadKind> } {
  const badRows = [...labels.keys()].sort((a, b) => a - b);
  const kinds: Record<string, BadKind> = {};
  for (const r of badRows) kinds[String(r)] = labels.get(r)!;
  return { badRows, kinds };
}
