import {STEPS, initial, advance, view} from './playground-model.mjs';
import recording from './search-recording.mjs';

const el = id => document.getElementById(id);
const stage = el('pg-stage');
const token = el('pg-token');
const nodes = Object.fromEntries([...stage.querySelectorAll('[data-node]')].map(n => [n.dataset.node, n]));
const motion = window.matchMedia('(prefers-reduced-motion: reduce)');
let state = initial();
let busy = false;

function node(tag, className, text) {
  const n = document.createElement(tag);
  if (className) n.className = className;
  if (text !== undefined) n.textContent = text;
  return n;
}

const buttons = STEPS.map((step, index) => {
  const button = node('button', 'pg-step');
  button.type = 'button';
  button.append(node('span', 'pg-step-num', String(index + 1)), node('strong', '', step.title), node('span', 'pg-step-detail', step.detail));
  button.addEventListener('click', () => run(index));
  const item = node('li');
  item.append(button);
  el('pg-steps').append(item);
  return button;
});

el('pg-question').textContent = recording.question;
el('pg-scope').textContent = `Top 3 of ${recording.documents} Glider docs pages`;
el('pg-results').append(...recording.results.map(doc => {
  const item = node('li');
  const link = node('a', '', doc.title);
  link.href = doc.url;
  item.append(link, node('p', '', doc.text));
  return item;
}));

function setTier(name, full) {
  nodes[name].classList.toggle('full', full);
}

function render() {
  const v = view(state);
  for (const name of ['ram', 'ssd', 's3']) setTier(name, v.tiers[name]);
  el('pg-machine').classList.toggle('down', v.server === 'Killed');
  el('pg-server-state').textContent = v.server;
  el('pg-caption').textContent = v.caption;
  el('pg-source').textContent = v.source.value;
  el('pg-source-note').textContent = v.source.note;
  el('pg-lost').textContent = String(v.lost);
  el('pg-lost-note').textContent = v.lostNote;
  el('pg-app').classList.toggle('pg-has-results', v.results);
  el('pg-empty').textContent = state.done === 4 ? 'The server is down. Restart it to query again.' : 'Results appear after the first query.';
  buttons.forEach((button, index) => {
    const next = index === state.done;
    button.classList.toggle('running', busy && next);
    button.classList.toggle('next', next && !busy);
    button.classList.toggle('done', index < state.done);
    button.setAttribute('aria-disabled', String(busy || !next));
    if (next) button.setAttribute('aria-current', 'step'); else button.removeAttribute('aria-current');
  });
  el('pg-reset').hidden = state.done === 0;
  el('pg-reset').setAttribute('aria-disabled', String(busy));
}

function center(name) {
  const box = nodes[name].getBoundingClientRect();
  const origin = stage.getBoundingClientRect();
  return [box.left - origin.left + box.width / 2, box.top - origin.top + box.height / 2];
}

async function travel(hops, after) {
  for (const hop of hops) {
    const [x0, y0] = center(hop.from);
    const [x1, y1] = center(hop.to);
    token.textContent = hop.label;
    token.hidden = false;
    await token.animate([
      {transform: `translate(calc(${x0}px - 50%), calc(${y0}px - 50%))`},
      {transform: `translate(calc(${x1}px - 50%), calc(${y1}px - 50%))`},
    ], {duration: 560, easing: 'cubic-bezier(.5,0,.3,1)', fill: 'forwards'}).finished;
    if (hop.to in after.tiers && after.tiers[hop.to]) setTier(hop.to, true);
  }
  token.hidden = true;
}

async function run(index) {
  if (busy || index !== state.done) return;
  const hadFocus = document.activeElement === buttons[index];
  const next = advance(state);
  const after = view(next);
  busy = true;
  render();
  stage.scrollIntoView({block: 'nearest', behavior: motion.matches ? 'auto' : 'smooth'});
  if (!motion.matches) {
    if (STEPS[index].id === 'crash') {
      el('pg-machine').classList.add('crashing');
      await new Promise(resolve => setTimeout(resolve, 650));
      el('pg-machine').classList.remove('crashing');
    } else {
      if (STEPS[index].id === 'restart') {
        el('pg-machine').classList.remove('down');
        el('pg-server-state').textContent = 'Taking over';
      }
      el('pg-caption').textContent = after.caption;
      await travel(after.hops, after);
    }
  }
  busy = false;
  state = next;
  render();
  if (hadFocus) (buttons[state.done] ?? el('pg-reset')).focus();
}

el('pg-reset').addEventListener('click', () => {
  if (busy) return;
  state = initial();
  render();
  buttons[0].focus();
});

render();
