/* Berkeley Looks Out · live link between phones and the projected wall.
   Uses public MQTT brokers over secure websockets (no account needed).
   Each person = one retained message on  berkeleylooksout/v1/<room>/p/<id>
   The id is a hash of the email, so the email itself is never sent. */
window.BLO = (function(){
  var qs = new URLSearchParams(location.search);
  var ROOM = (qs.get('room') || 'jacobs-hall').replace(/[^a-z0-9_-]/gi, '').toLowerCase() || 'jacobs-hall';
  var BROKERS = (qs.get('broker') || 'wss://broker.hivemq.com:8884/mqtt,wss://test.mosquitto.org:8081/mqtt').split(',');
  var ROOT = 'berkeleylooksout/v1/' + ROOM + '/';
  var BASE = ROOT + 'p/';
  /* meeting places offered when someone accepts a request — edit freely */
  var SPOTS = (qs.get('spots') ? qs.get('spots').split('|') : ['Lobby · by the resource wall','Makerspace · ground floor','Commons · 2nd floor','Studios · 3rd floor','Front steps · outside']);
  var FA = 'f‌irst aid';
  var KINDS = [['kit',FA],['helmet','NERT · verified'],['bike','bike repairs'],['wrench','basic repairs'],['plant','plant care'],['laptop','tech help'],['pot','cooking'],['books','study help'],['camera','photography'],['bubble','speaks a language'],['tent','camping gear'],['heart','listening ear']];
  function label(k){ for (var i=0;i<KINDS.length;i++) if (KINDS[i][0]===k) return KINDS[i][1]; return k; }

  function fallbackHash(s){ var h1=0x811c9dc5, h2=0x01000193; for (var i=0;i<s.length;i++){ h1=Math.imul(h1^s.charCodeAt(i),16777619); h2=Math.imul(h2+s.charCodeAt(i),2654435761); } return ((h1>>>0).toString(16)+(h2>>>0).toString(16)).padStart(16,'0'); }
  function uidFor(email){
    var s = email.trim().toLowerCase() + '|berkeley-looks-out';
    if (window.crypto && crypto.subtle && window.isSecureContext){
      return crypto.subtle.digest('SHA-256', new TextEncoder().encode(s)).then(function(b){
        return Array.from(new Uint8Array(b)).slice(0,8).map(function(x){ return x.toString(16).padStart(2,'0'); }).join('');
      });
    }
    return Promise.resolve(fallbackHash(s));
  }

  var clients = [], subs = [];
  function subscribe(suffix){ if (subs.indexOf(suffix) < 0) subs.push(suffix); clients.forEach(function(c){ if (c.connected) c.subscribe(ROOT + suffix, {qos:1}); }); }
  function connect(opts){
    opts = opts || {};
    var up = 0;
    function status(){ opts.onStatus && opts.onStatus(up, clients.length); }
    clients = BROKERS.map(function(url){
      var c = mqtt.connect(url, {clientId:'blo-' + Math.random().toString(16).slice(2,10), reconnectPeriod:3000, connectTimeout:8000, clean:true, keepalive:30});
      c._up = false;
      c.on('connect', function(){
        if (!c._up){ c._up = true; up++; status(); }
        if (opts.subscribe) c.subscribe(BASE + (opts.subscribe === true ? '+' : opts.subscribe), {qos:1});
        subs.forEach(function(s){ c.subscribe(ROOT + s, {qos:1}); });
      });
      function down(){ if (c._up){ c._up = false; up--; status(); } }
      c.on('close', down); c.on('offline', down); c.on('error', function(){});
      c.on('message', function(topic, buf){
        if (!opts.onMessage) return;
        var data = null;
        try { data = JSON.parse(buf.toString() || 'null'); } catch(e){ return; }
        var rel = topic.slice(ROOT.length);
        if (rel.indexOf('p/') === 0) opts.onMessage(rel.slice(2), data, rel);
        else if (opts.onOther) opts.onOther(rel, data);
      });
      return c;
    });
    status();
    return clients;
  }
  function send(suffix, obj){ // retained; obj null deletes it
    var payload = obj == null ? '' : JSON.stringify(obj);
    clients.forEach(function(c){ c.publish(ROOT + suffix, payload, {retain:true, qos:1}); });
  }
  function publish(id, obj){ send('p/' + id, obj); }
  return {ROOM:ROOM, ROOT:ROOT, BASE:BASE, KINDS:KINDS, SPOTS:SPOTS, label:label, uidFor:uidFor, connect:connect, publish:publish, send:send, subscribe:subscribe};
})();

/* pixel icons shared by the phone and the wall */
window.BLO_ICONS = {
  kit:["....####....","....#..#....","############","#..........#","#....##....#","#....##....#","#..######..#","#..######..#","#....##....#","#....##....#","#..........#","############"],
  helmet:[".....####.....","...##.##.##...","..#...##...#..",".#....##....#.",".#....##....#.","#.....##.....#","##############","#............#",".############."],
  bike:["..........###...","...###.....#....","....#.....#.....","....######......","...#.#...#.#....",".###..#.#..###..","#...#..#..#...#.","#..#.#...#.#..#.","#...#.....#...#.",".###.......###.."],
  pot:["..#..#..#...","...#..#..#..","............","...######...",".##########.","#.#......#.#","..#......#..","..#......#..","..#......#..","...######..."],
  plant:[".......###.",".....##..#.","..##.#..#..",".#..##.#...",".#...##....","..#..#.....","...###.....",".....#.....",".....#.....","...#####...","...#...#...","....###...."],
  books:["############","#..........#","############",".##########.",".#........#.",".##########.","############","#..........#","############"],
  laptop:["..##########..","..#........#..","..#........#..","..#........#..","..#........#..","..##########..",".############.","#............#","##############"],
  bubble:[".##########.","#..........#","#.########.#","#..........#","#.######...#","#..........#",".###..#####.","...#.#......","...##.......","...#........"],
  wrench:["........##..",".......#..#.",".......#..##","......#....#",".....#...##.","....#...#...","...#...#....","..#...#.....",".#...#......","#...#.......","#..#........",".##........."],
  camera:["....####.....","############.","#..........#.","#...####...#.","#..#....#..#.","#..#....#..#.","#...####...#.","#..........#.","############."],
  tent:["......#......",".....#.#.....","....#...#....","...#..#..#...","..#..#.#..#..",".#..#...#..#.","#..#.....#..#","#############"],
  heart:[".###...###.","#...#.#...#","#....#....#","#.........#",".#.......#.","..#.....#..","...#...#...","....#.#....",".....#....."],
  plus:["....##....","....##....","....##....","##########","##########","....##....","....##....","....##...."]
};
