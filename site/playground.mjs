import {points, metrics, search, commands} from './playground-model.mjs';

const $ = id => document.getElementById(id);
const colors = {docs: '#1E3A5F', memory: '#F07A1A', ops: '#C9D2DC'};
const svgNS = 'http://www.w3.org/2000/svg';
const map = $('pg-map');
const px = x => 70 + x * 34;
const py = y => 370 - y * 34;
function svg(tag, attributes, label) {
  const node = document.createElementNS(svgNS, tag);
  for (const [key, value] of Object.entries(attributes)) node.setAttribute(key, value);
  if (label !== undefined) node.textContent = label;
  return node;
}

function draw(vector, eligible, results) {
  const nodes = [];
  for (let i = 0; i <= 10; i += 2) {
    nodes.push(svg('line', {x1: px(i), y1: py(0), x2: px(i), y2: py(10), class: 'pg-gridline'}));
    nodes.push(svg('line', {x1: px(0), y1: py(i), x2: px(10), y2: py(i), class: 'pg-gridline'}));
    nodes.push(svg('text', {x: px(i), y: 390, 'text-anchor': 'middle'}, i));
    nodes.push(svg('text', {x: 54, y: py(i) + 4, 'text-anchor': 'end'}, i));
  }
  nodes.push(svg('text', {x: 445, y: 390}, 'X'));
  nodes.push(svg('text', {x: 50, y: 18}, 'Y'));
  const ranks = new Map(results.map((hit, i) => [hit.id, i + 1]));
  const included = new Set(eligible.map(point => point.id));
  const validQuery = vector.every(Number.isFinite);
  if (validQuery) {
    for (const point of points.filter(p => ranks.has(p.id))) {
      nodes.push(svg('line', {x1: px(vector[0]), y1: py(vector[1]), x2: px(point.vector[0]), y2: py(point.vector[1]), class: 'pg-link'}));
    }
  }
  for (const point of points) {
    const hit = ranks.has(point.id);
    const group = svg('g', {class: included.has(point.id) ? '' : 'pg-excluded'});
    group.append(svg('circle', {cx: px(point.vector[0]), cy: py(point.vector[1]), r: hit ? 9 : 6,
      fill: colors[point.metadata.kind], class: hit ? 'pg-point pg-point-hit' : 'pg-point'}));
    group.append(svg('title', {}, `Point ${point.id}: [${point.vector.join(', ')}], ${point.metadata.kind}, price ${point.metadata.price}${hit ? `, rank ${ranks.get(point.id)}` : ''}`));
    if (hit) group.append(svg('text', {x: px(point.vector[0]) + 13, y: py(point.vector[1]) - 9, class: 'pg-rank'}, `#${point.id}`));
    nodes.push(group);
  }
  if (validQuery) {
    const [x, y] = [px(vector[0]), py(vector[1])];
    nodes.push(svg('path', {d: `M${x} ${y - 10} L${x + 10} ${y} L${x} ${y + 10} L${x - 10} ${y} Z`, class: 'pg-query'}));
    nodes.push(svg('text', {x, y: y + 26, 'text-anchor': 'middle', class: 'pg-rank'}, 'query'));
  }
  map.replaceChildren(...nodes);
}

function options() {
  return {
    vector: [$('pg-x').valueAsNumber, $('pg-y').valueAsNumber],
    metric: $('pg-metric').value, k: Number($('pg-k').value),
    kind: $('pg-kind').value, maxPrice: $('pg-price').value === 'all' ? null : Number($('pg-price').value),
  };
}

function render() {
  const current = options();
  $('pg-metric-note').textContent = metrics[current.metric].note;
  try {
    if (current.vector.some(x => !Number.isFinite(x) || x < 0 || x > 10)) {
      throw new Error('Enter X and Y between 0 and 10.');
    }
    const {eligible, results} = search(current);
    $('pg-error').hidden = true;
    $('pg-x').removeAttribute('aria-invalid');
    $('pg-y').removeAttribute('aria-invalid');
    $('pg-results').replaceChildren(...results.map(hit => {
      const row = document.createElement('tr');
      for (const value of [`#${hit.id}`, `${hit.metadata.kind} / ${hit.metadata.price}`, hit.distance.toFixed(4)]) {
        const cell = document.createElement('td');
        cell.textContent = value;
        row.append(cell);
      }
      return row;
    }));
    $('pg-count').textContent = `${results.length} hits / ${eligible.length} eligible`;
    $('pg-empty').hidden = results.length > 0;
    const code = commands(current);
    $('pg-query-code').textContent = code.query;
    $('pg-setup-code').textContent = code.setup;
    document.querySelectorAll('#pg-app .copy').forEach(button => {button.disabled = false;});
    draw(current.vector, eligible, results);
  } catch (error) {
    $('pg-error').textContent = error.message;
    $('pg-error').hidden = false;
    $('pg-x').setAttribute('aria-invalid', 'true');
    $('pg-y').setAttribute('aria-invalid', 'true');
    $('pg-results').replaceChildren();
    $('pg-count').textContent = 'Invalid query';
    $('pg-empty').hidden = true;
    $('pg-query-code').textContent = '# Fix the query vector to generate a request.';
    document.querySelectorAll('#pg-app .copy').forEach(button => {button.disabled = true;});
    draw([NaN, NaN], [], []);
  }
}

function moveQuery(x, y) {
  $('pg-x').value = (Math.round(Math.max(0, Math.min(10, x)) * 10) / 10).toFixed(1);
  $('pg-y').value = (Math.round(Math.max(0, Math.min(10, y)) * 10) / 10).toFixed(1);
  render();
}
map.addEventListener('click', event => {
  const point = map.createSVGPoint();
  point.x = event.clientX;
  point.y = event.clientY;
  const local = point.matrixTransform(map.getScreenCTM().inverse());
  moveQuery((local.x - 70) / 34, (370 - local.y) / 34);
});
map.addEventListener('keydown', event => {
  const delta = {ArrowLeft: [-0.1, 0], ArrowRight: [0.1, 0], ArrowUp: [0, 0.1], ArrowDown: [0, -0.1]}[event.key];
  if (!delta) return;
  event.preventDefault();
  const [x, y] = options().vector;
  moveQuery(Number.isFinite(x) ? x + delta[0] : 5, Number.isFinite(y) ? y + delta[1] : 5);
});
$('pg-form').addEventListener('input', render);
$('pg-form').addEventListener('change', render);
$('pg-form').addEventListener('submit', event => {event.preventDefault(); render();});
const presets = {
  neighbors: {vector: [6.5, 4.5], metric: 'squared_euclidean', kind: 'all', price: 'all'},
  filter: {vector: [6.5, 4.5], metric: 'squared_euclidean', kind: 'docs', price: '3'},
  direction: {vector: [2, 2], metric: 'cosine', kind: 'all', price: 'all'},
};
document.querySelectorAll('[data-preset]').forEach(button => {
  button.addEventListener('click', () => {
    const preset = presets[button.dataset.preset];
    $('pg-metric').value = preset.metric;
    $('pg-kind').value = preset.kind;
    $('pg-price').value = preset.price;
    $('pg-k').value = '5';
    moveQuery(...preset.vector);
  });
});
render();
$('pg-app').hidden = false;
