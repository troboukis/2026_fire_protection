import { westAtticaMessages } from './west_attica_messages.mjs'

const virtualId = '\0firewatch-west-attica-translations'
export function replaceOnce(source, before, after, label) {
  if (source.split(before).length !== 2) throw new Error(`West Attica source changed: ${label}. Review the language integration.`)
  return source.replace(before, after)
}

// These are transformations of maintained TypeScript source at build time.
// Published JavaScript is regenerated; it is never patched by hand.
export function westAtticaLanguage() {
  const messages = westAtticaMessages()
  return {
    name: 'firewatch-west-attica-language', enforce: 'pre',
    resolveId(id) { if (id === 'firewatch:west-attica-translations') return virtualId },
    load(id) {
      if (id !== virtualId) return
      return `const texts=${JSON.stringify(messages.texts)}, organizations=${JSON.stringify(messages.organizations)};
export function nativeText(source,lang='en') {
  if(lang==='el'||typeof source!=='string')return source;
  const match=source.match(/^(\\s*)([\\s\\S]*?)(\\s*)$/);
  return texts[match[2]] ? match[1]+texts[match[2]]+match[3] : source;
}
export function nativeOrganization(source,lang){return lang==='en' ? organizations[source]??source : source;}
export function nativeCaption(source,lang){return source.split(/(<[^>]*>)/).map(part=>part.startsWith('<')?part:nativeText(part,lang)).join('');}`
    },
    transform(source, id) {
      const file = id.split('?')[0].replaceAll('\\', '/')
      if (!file.includes('/mega_fire_2026/app/src/')) return
      if (file.endsWith('/src/App.tsx')) {
        source = replaceOnce(source, 'const [lang] = useState<Lang>("el");', `const [lang, setLang] = useState<Lang>(() => window.location.pathname.startsWith('/en/') ? 'en' : 'el');
  useEffect(() => {
    const update = () => setLang(window.location.pathname.startsWith('/en/') ? 'en' : 'el');
    window.addEventListener('firewatch-language-change', update);
    window.addEventListener('popstate', update);
    return () => { window.removeEventListener('firewatch-language-change', update); window.removeEventListener('popstate', update); };
  }, []);
  useEffect(() => {
    document.documentElement.lang = lang;
  }, [lang, eventId]);`, 'language state')
        source = replaceOnce(source, '{t.failedToLoad} {error}', '{t.failedToLoad}', 'product error copy')
        source = replaceOnce(source, 'const t = UI[lang];', `const t = UI[lang];
  const activeLightboxPhoto = lightboxPhoto && { ...lightboxPhoto, caption: ANALYSIS_CONTENT[eventId][lang].body.flatMap(block => block.type === 'photo' && block.src === lightboxPhoto.src ? [block.caption] : [])[0] ?? lightboxPhoto.caption };`, 'active photo')
        // Read the caption in the current language while retaining the same photo.
        const start = source.indexOf('      {lightboxPhoto && (')
        if (start < 0) throw new Error('West Attica lightbox source changed.')
        source = source.slice(0, start) + source.slice(start).replaceAll('lightboxPhoto', 'activeLightboxPhoto')
        source = source.replaceAll('alt="WWF Ελλάς"', 'alt={lang === "en" ? "WWF Greece" : "WWF Ελλάς"}')
        source = replaceOnce(source, 'import firewatchLogo from "./assets/logos/firewatch.png";', 'import firewatchLogo from "./assets/logos/firewatch.png";\nimport firewatchEnglishLogo from "firewatch:english-logo";', 'bilingual logo')
        source = source.replaceAll('src={firewatchLogo}', 'src={lang === "en" ? firewatchEnglishLogo : firewatchLogo}')
        return source
      }
      if (file.endsWith('/lib/i18n.ts')) return source + `
import { nativeText } from 'firewatch:west-attica-translations';
for (const [key,value] of Object.entries(UI.el)) if(typeof value==='string') (UI.en as unknown as Record<string,string>)[key]=nativeText(value);
`
      if (file.endsWith('/lib/analysis.ts')) return source + `
import { nativeText, nativeCaption } from 'firewatch:west-attica-translations';
const original=ANALYSIS_CONTENT['attica-boeotia-2026'].el;
ANALYSIS_CONTENT['attica-boeotia-2026'].en={...original,eyebrow:nativeText(original.eyebrow),title:nativeText(original.title),date:nativeText(original.date),body:original.body.map(block=>block.type==='photo'?{...block,caption:nativeCaption(block.caption,'en')}:block.type==='ul'?{...block,items:block.items.map(item=>nativeText(item))}:{...block,text:nativeText(block.text)})};
`
      if (file.endsWith('/lib/antinero.ts')) {
        if (!source.includes('ANTINERO_LABELS_EN[category as AntineroCategory]')) throw new Error('West Attica category labels changed.')
        source = replaceOnce(source, '"Other antinero work"', 'nativeText("Λοιπό έργο αντιπυρικής προστασίας")', 'category fallback')
        return `import { nativeText } from 'firewatch:west-attica-translations';\n` + source + '\nfor (const category of ANTINERO_CATEGORIES) ANTINERO_LABELS_EN[category] = nativeText(category);\n'
      }
      if (file.endsWith('/components/FireMap.tsx')) {
        source = replaceOnce(source, '[t.popupAuthority, props.authority_name]', '[t.popupAuthority, nativeOrganization(props.authority_name, lang)]', 'authority display')
        source = replaceOnce(source, 'title: props.point_name ?? t.popupWork', 'title: nativeText(props.point_name, lang) ?? t.popupWork', 'place display')
        source = replaceOnce(source, 'props.work_type !== props.category ? props.work_type : null', 'props.work_type !== props.category ? nativeText(props.work_type, lang) : null', 'work taxonomy display')
        source = replaceOnce(source, '  desc?: unknown;', "  desc?: unknown;\n  descLang?: 'el';", 'source title language')
        source = replaceOnce(source, 'desc: props.work,', "desc: props.work, descLang: 'el',", 'original contract title')
        source = replaceOnce(source, '<div class="map-popup__desc">', '<div class="map-popup__desc"${opts.descLang ? \' lang="el"\' : \'\'}>', 'source title markup')
        source = replaceOnce(source, 'lang === "el" ? haToStremmata(emsrTotalHa) : emsrTotalHa', 'haToStremmata(emsrTotalHa)', 'EMSR display unit')
        const area = 'lang === "el" ? haToStremmata(props.area_ha) : props.area_ha'
        if (source.split(area).length !== 4) throw new Error('West Attica area displays changed.')
        source = source.replaceAll(area, 'haToStremmata(props.area_ha)')
        return `import { nativeText, nativeOrganization } from 'firewatch:west-attica-translations';\n` + source
      }
    },
  }
}
