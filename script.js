// Import Firebase modules
        import { initializeApp } from "https://www.gstatic.com/firebasejs/11.6.1/firebase-app.js";
        import { getAuth, signInAnonymously, onAuthStateChanged, GoogleAuthProvider, signInWithPopup, signOut } from "https://www.gstatic.com/firebasejs/11.6.1/firebase-auth.js";
        import { getFirestore, doc, setDoc, onSnapshot, collection, getDoc, getDocs, deleteDoc, serverTimestamp, writeBatch } from "https://www.gstatic.com/firebasejs/11.6.1/firebase-firestore.js";
                import { estadisticas, estadisticasDelitos } from './data.js';
        
        // --- CONFIGURATION -- -
        const firebaseConfig = typeof __firebase_config !== 'undefined' ? JSON.parse(__firebase_config) :  {
  apiKey: "AIzaSyCUCGqhsRJiIsen4zSeelCzmrBA_l-7K4A",
  authDomain: "codisec-st.firebaseapp.com",
  projectId: "codisec-st",
  storageBucket: "codisec-st.firebasestorage.app",
  messagingSenderId: "1006950405629",
  appId: "1:1006950405629:web:fdb7f3f92e6f80a786f594"
};
        const appId = typeof __app_id !== 'undefined' ? __app_id : 'default-serenazgo-app';
        const AUTHORIZED_EMAILS = ['codisecperu@gmail.com', 'gerson.angulo.zavaleta@gmail.com'];

        // --- FIREBASE INITIALIZATION -- -
        const app = initializeApp(firebaseConfig);
        const db = getFirestore(app);
        const auth = getAuth(app);
        const googleProvider = new GoogleAuthProvider();
        
        let userId = null;
        let userEmail = null;
        let isAuthorized = false;
        let dbListeners = []; 
        let currentData = {};
        let charts = {};

        const months = ["Enero", "Febrero", "Marzo", "Abril", "Mayo", "Junio", "Julio", "Agosto", "Setiembre", "Octubre", "Noviembre", "Diciembre"];

        const delitosComisariaData = [
            { mes: 'enero', chorrillos_hurto: 47, villa_hurto: 68, mateo_hurto: 10, san_genaro_hurto: 20, chorrillos_robo: 12, villa_robo: 28, mateo_robo: 11, san_genaro_robo: 2 },
            { mes: 'febrero', chorrillos_hurto: 29, villa_hurto: 80, mateo_hurto: 7, san_genaro_hurto: 10, chorrillos_robo: 8, villa_robo: 27, mateo_robo: 10, san_genaro_robo: 2 },
            { mes: 'marzo', chorrillos_hurto: 26, villa_hurto: 108, mateo_hurto: 13, san_genaro_hurto: 16, chorrillos_robo: 7, villa_robo: 44, mateo_robo: 11, san_genaro_robo: 6 },
            { mes: 'abril', chorrillos_hurto: 25, villa_hurto: 66, mateo_hurto: 9, san_genaro_hurto: 8, chorrillos_robo: 12, villa_robo: 27, mateo_robo: 4, san_genaro_robo: 2 },
            { mes: 'mayo', chorrillos_hurto: 24, villa_hurto: 64, mateo_hurto: 14, san_genaro_hurto: 15, chorrillos_robo: 8, villa_robo: 40, mateo_robo: 3, san_genaro_robo: 6 },
            { mes: 'junio', chorrillos_hurto: 11, villa_hurto: 63, mateo_hurto: 4, san_genaro_hurto: 11, chorrillos_robo: 15, villa_robo: 33, mateo_robo: 17, san_genaro_robo: 9 },
            { mes: 'julio', chorrillos_hurto: 8, villa_hurto: 4, mateo_hurto: 19, san_genaro_hurto: 10, chorrillos_robo: 4, villa_robo: 41, mateo_robo: 8, san_genaro_robo: 2 },
        ];

        const slugify = text => {
            let slug = text.toString().toLowerCase().replace(/\s+/g, '-').replace(/[^\w\-]+/g, '').replace(/\-\-+/g, '-').replace(/^-+/, '').replace(/-+$/, '');
            if (/^\d/.test(slug)) {
                slug = 'id-' + slug;
            }
            return slug;
        };

        // --- APPLICATION SECTIONS DEFINITION -- -
        const sections = {
            'aprobaciones': { title: 'Aprobaciones', type: 'custom', renderer: renderAprobacionesSection, authorizedOnly: true },
            'estadisticas_generales': {
                type: 'long_table', title: 'Estadísticas Generales', collection: 'estadisticas_generales',
                groups: [
                    { title: "01 EMISION DE ALERTAS TEMPRANAS EN APOYO A LA PNP ,  EN  ACTIVIDADES   PRESUNTAMENTE    DELICTIVAS", sub_groups: [
                        { title: "0101 PRESUNTAS ACTIVIDADES CONTRA LA VIDA EL CUERPO Y LA SALUD", rows: ["PRESUNTO HOMICIDIO", "PRESUNTO FEMINICIDIO", "PRESUNTO SICARIATO", "PRESUNTAS LESIONES LEVES Y  GRAVES", "PRESUNTA EXPOSICION A PELIGRO O ABANDONO DE PERSONAS (EXPOSICION, ABANDONO, OMISION DE SOCORRO Y AUXILIO)."] },
                        { title: "0102 PRESUNTA ACTIVIDAD CONTRA LA LIBERTAD", rows: ["PRESUNTO CONTRA LA LIBERTAD PERSONAL (ACOSO, SECUESTRO)", "PRESUNTA TRATA DE PERSONAS", "PRESUNTA VIOLACION LIBERTAD SEXUAL (MAYOR Y MENOR DE EDAD, SEDUCCION, PORNOGRAFIA, TOCAMIENTOS,  ACOSO, EXHIBICIONISMO Y CHANTAJE)", "OTROS PRESUNTOS (LA INTIMIDAD, VIOLACION DE DOMICILIO, COMUNICACIONES, SECRETO PROFESIONAL, DE REUNION, TRABAJO, EXPRESION, PROXENETISMO Y OFENSAS AL PUDOR)"] },
                        { title: "0103 PRESUNTAS ACTIVIDADES CONTRA   EL   PATRIMONIO", rows: ["PRESUNTO ROBO A PERSONAS (CELULARES, MOCHILAS, CARTERAS, BILLETERAS, RELOGES, JOYAS Y OTROS)", "PRESUNTO ROBO DE CASA HABITADA", "PRESUNTO ROBO DE GANADO (ABIGEATO)", "PRESUNTO ROBO A EMPRESAS PARTICULARES Y ESTATALES", "PRESUNTO ROBO DE VEHÍCULOS (MAYORES Y MENORES)", "PRESUNTO ROBO DE ACCESORIOS Y AUTOPARTES", "PRESUNTO ROBO A PASAJEROS EN VEHICULOS DE TRANSPORTE Y MERCANCIAS EN VEHICULOS DE CARGA Y REPARTIDORES", "PRESUNTO DAÑO (DAÑAR, DESTRUIR O INUTILIZAR BIEN MUEBLE O INMUEBLE)", "PRESUNTO HURTO A PERSONAS (CELULARES, MOCHILAS, CARTERAS, BILLETERAS, RELOJES, JOYAS Y OTROS)", "HURTO DE CASA HABITADA", "PRESUNTO HURTO DE GANADO (ABIGEATO)", "PRESUNTO HURTO A EMPRESAS PARTICULARES Y ESTATALES", "PRESUNTO HURTO DE VEHÍCULOS (MAYORES Y MENORES)", "PRESUNTO HURTO A PASAJEROS EN VEHICULOS DE TRANSPORTE Y MERCANCIAS EN VEHICULOS DE CARGA Y REPARTIDORES", "PRESUNTO OTROS (RECEPTACION, APROPIACION ILICITA, ESTAFA, EXTORSION, CHANTAJE Y USURPACION DE INMUEBLE)"] },
                        { title: "0104 PRESUNTAS ACTIVIDADES CONTRA LA SEG. PUB.", rows: ["PRESUNTO PELIGRO COMUN (CREAR INCENDIO O EXPLOSION, CONDUCCION O MANIPULACION EN ESTADO DE EBRIEDAD O DROGADICCIÓN, FABRICACIÓN, TRAFICO O TENECIA DE ARMAS, EXPLOSIVOS Y PIROTECNICOS)", "PRESUNTO CONTRA EL TRANSPORTE (CONTRA LA SEGURIDAD COMUN Y ENTORPECIMIENTO DEL SERVICIO)"] },
                        { title: "0105 PRE. ACTIV.CONTRA LA SALUD PUB.", rows: ["PRESUNTA CONTAMINACION Y PROPAGACION (CONTAMINACION DE AGUAS, ADULTERACION DE SUSTANCIAS Y COMERCIALIZACION DE PRODUCTOS NOCIVOS)", "PRESUNTO TRAFICO ILICITO DE DROGAS (PROMOCION, FAVORECIMIENTO, COMERCIALIZACION, PRODUCCION, MICROCOMERCIALIZACION Y POSESION)."] },
                        { title: "0106 PRE. ACTIV. AMBIENTALES", rows: ["PRESUNT ACTIVIDAD CONTRA LOS RECURSOS NATURALES Y MEDIO AMBIENTE (CONTAMINACION, RESIDUOS PELIGROSOS Y  MINERIA ILEGAL, )", "PRESUNTA ACTIVIDAD CONTRA LOS RECURSOS NATURALES (TRAFICO ILEGAL DE ESPECIES ACUATICAS, FLORA Y FAUNA, TALA ILEGAL Y DEFORESTACON DE BOSQUES)"] },
                        { title: "0107 PRESUNTA ACTIVIDAD CONTRA LA TRANQUILIDAD PUB", rows: ["PRESUNTA ACTIVIDAD CONTRA LA PAZ PUBLICA ( DISTURBIOS, APOLOGIA, ORG. CRIMINAL, BANDA Y REGLAJE O MARCAJE)", "PRESUNTA ACTIVIDAD TERRORISTA (PROVOCA, CREA O MANTIENE ESTADO DE ZOZOBRA, ALARMA O TERROR A LA POBLACION)", "OTROS PRESUNTOS (DISCRIMINACION, CONTRA LA HUMANIDAD, GENOCIDIO, DESAPARICION FORZADA Y TORTURA)"] },
                        { title: "0108 PRESUNTAS ACTIV. CONTRA LA ADM. PUB.", rows: ["COMETIDOS POR FUNCIONARIOS PÚBLICOS (ABUSO DE AUTORIDAD, MALVERSACIÓN, OTROS)", "COMETIDOS POR PARTICULARES (USURPACION DE AUTORIDAD, VIOLENCIA Y RESISTENCIA, OTROS)"] },
                        { title: "0109 PRE. ACTIV. CONTRA LOS PODERES DEL ESTADO", rows: ["REBELION (ALZARSE EN ARMAS), SEDICION (DESCONOCER AL GOBIERNO) Y MOTIN (EMPLEAR LA VIOLENCIA Y ATRIBUIRSE DERECHOS DEL PUEBLO Y HACER PETICIONES EN NOMBRE DE ESTE)"] } ]
                    },
                    { title: "02 EMISION DE ALERTAS TEMPRANAS EN APOYO A LA PNP EN  PRESUNTAS  FALTAS", sub_groups: [
                            { title: "0201 PRESUNTA ACTIV. CONTRA CONTRA LA PERSONA", rows: ["PRESUNTAS LESIONES (MUY LEVES)", "PRESUNTO MALTRATO (SIN LESIÓN)", "PRESUNTAS PELEAS CALLEJERAS CON LESIONES MUY LEVES O SIN LESIONES)", "PRESUNTA MORDEDURA CANINA, PATADA DE CABALLO O LESIÓN DE ANIMAL DOMESTICO"] },
                            { title: "0202 PRESUNTA ACTIV. CONTRA EL PATRIMONIO", rows: ["HURTO SIMPLE COD. PENAL PERUANO ART. 444 CUANDO LA ACCION RECAE SOBRE UN BIEN CUYO VALOR NO SOBRE PASE EL 10 % DE LA UIT.", "PRESUNTO DAÑO A LA PROPIEDAD MUEBLE O INMUEBLE", "PRESUNTO HURTO  FAMÉLICO (CONSUMIR Y NO PAGAR)", "PRESUNTA USURPACION BREVE Y JUEGOS PROHIBIDOS."] },
                            { title: "0203 PRE. ACTIV. CONTRA LA SEG. PÚBLICA", rows: ["ARROJO DE BASURA O QUEMARLA CAUSANDO MOLESTIAS", "PRESUNTA OBSTRUCCIÓN Y ARROJO DE ESCOMBROS Y/O MATERIALES. TAMBIEN NO COLOCAR SEÑALES PREVENTIVAS EN LA VIA.", "CONDUCIR VEHÍCULO NO MOTORIZADO O ANIMAL A EXCESIVA VELOCIDAD /O CONFIAR VEHICULO A MENOR DE EDAD O INEXPERTO"] },
                            { title: "0204 PRESUNTA ACTIV. CONTRA LAS BUENAS COSTUMBRES", rows: ["PRESUNTO CONSUMO DE ALCOHOL O DROGAS PERTURBANDO LA TRANQUILIDAD O SEGURIDAD DE LAS PERSONAS O SUMINISTRARLO A MENORES DE EDAD.", "PRESUNTA DESTRUCCIÓN DE PLANTAS", "PRESUNTO ACTOS DE CRUELDAD CONTRA LOS ANIMALES", "PRESUNTA PROPOSICION INMORAL O DESHONESTA"] },
                            { title: "0205 PRESUNTA ACTIV. CONTRA LA TRANQUIL. PUB.", rows: ["PERTURBAR ACTOS SOLEMNES O REUNIONES PÚBLICAS", "FALTAMIENTO DE PALABRA A UNA AUTORIDAD", "OCULTAR SU NOMBRE, DOMICILIO O ESTADO CIVIL A LA AUTORIDAD", "EL QUE NIEGA AUXILIO A UNA AUTORIDAD PARA SOCORRER A UN TERCERO", "PERTURBAR CON DISCUSIONES RUIDOS O MOLESTIAS ANALOGAS"] } ]
                    },
                    { title: "03 EMISION DE ALERTAS TEMPRANAS EN APOYO A LA PNP Y OTRAS GERENCIAS, EN PRESUNTAS  INFRACCIONES", sub_groups: [
                            { title: "0301 PRESUNTOS ACCIDENTES E INFRACCIONES AL TRÁNSITO Y TRANSPORTE", rows: ["PRESUNTO ATROPELLO", "PRESUNTO ATROPELLO Y FUGA", "PRESUNTO CHOQUE", "PRESUNTO CHOQUE Y FUGA", "OTROS PRESUNTOS ACCIDENTES (DESPISTE, VOLCADURA, CAIDA DE PASAJERO E INCENDIO)", "APOYO SUBSIDIARIO A LA PNP , PARA EL CONTROL DE TRANSITO VEHICULAR CUANDO LO SOLICITE", "PRESUNTO VEHÍCULO ABANDONADO", "VEHÍCULOS NO AUTORIZADOS O PROHIBIDOS PARA SERVICIO PÚBLICO", "VEHÍCULOS QUE HACEN PIQUES Y CARRERAS.", "PARADEROS Y VEHÍCULOS DE TRANSPORTE PÚBLICO INFORMALES", "VEHÍCULOS ESTACIONADOS EN ZONA PROHIBIDA", "OTRAS INFRACCIONES AL RGTO. DE TRÁNSITO."] },
                            { title: "0302 PRESUNTA ACTIV.  CONTRA LA LEY DE PROTEC. Y  BIENESTAR ANIMAL", rows: ["LEY Nº 30407 - ANIMALES SENSIBLES , DOMESTICOS, DE GRANJA, SILVESTRES Y ACUÁTICOS Y ANIMALES DE COMPAÑIA O MASCOTAS. (ABANDONO DE ANIMALES EN LA VIA PUBLICA - UTILIZACION DE ANIMALES EN ESPECTACULOS DE ENTRETENIMIENTO PUBLICO O PRIVADO - LA TENENCIA, CAZA, CAPTURA, COMPRA Y VENTA PARA EL CONSUMO HUMANO DE ESPECIES ANIMALES NO DEFINIDAS COMO ANIMALES DE GRANJA, EXCEPTUANDOSE AQUELLAS SILVESTRES CRIADAS EN ZOOCRIADEROS O PROVENIENTES DE AREAS DE MANEJO AUTORIZADAS Y PELEAS DE ANIMALES  TANTO DOMESTICOS COMO SILVESTRES EN LUGARES PUBLICOS O PRIVADOS)"] },
                            { title: "0303 PRESUNTA ACTIV. DE PERSONAS QUE AFECTAN TRANQUILIDAD Y EL ORDEN", rows: ["CAMBISTAS EN LA VÍA PÚBLICA (INTERRUMPEN TRÁNSITO PEATONAL Y VEHICULAR)", "LLAMADORES,  JALADORES Y PARQUEADORES  INFORMALES (QUE GRITAN Y SE APROPIAN DE ESPACIOS PUBLICOS)", "RECICLADORES FORMALES O INFORMALES", "ORATES Y/O INDIGENTES (AGRESIVOS, QUE EXIGEN DINERO O SE POSESIONAN DE ESPACIOS, INTERRUMPIENDO ACTIVIDADES PUBLICAS O PRIVADAS)", "PRESUNTAS PERSONAS EN ACTITUD SOSPECHOSA", "PRESUNTAS PERSONAS EN VEHÍCULOS SOSPECHOSOS", "PRESUNTOS TRABAJadores SEXUALES  EN LA VIA PÚBLICA (MASCULINO O FEMENINO)", "PRESUNTAS PERSONAS QUE MICCIONAN EN VIA PÚBLICA", "OTROS PRESUNTOS (ESPECIFICAR EN EL ACAPITE DATOS IMPORTANTES DEL FORMATO DE OCURRENCIAS)"] },
                            { title: "0304 PRESUNTAS INFRACCIONES A LAS ORDENANZAS Y LICENCIAS MUNICIPALES", rows: ["COMERCIO EN VÍA PÚBLICA DISTINTA AL AUTORIZADO", "COMERCIO EN VÍA PÚBLICA NO AUTORIZADO", "COMERCIO SIN LICENCIA DE FUNCIONAMIENTO", "CONSTRUCCIONES SIN LICENCIA MUNICIPAL", "TRABAJO SEXUAL CLANDESTINO EN INMUEBLE (SALA DE MASAJES, SAUNAS Y OTROS)", "ESTABLECIMIENTOS EN MALAS CONDICIONES DE LIMPIEZA", "RUIDOS MOLESTOS", "OBSTRUCCION DE CALZADA, VEREDA Y OTROS, SIN INDICAR VIA ALTERNA / SIN SEÑALIZACION / SIN MEDIDAS DE SEGURIDAD (CONSTRUCCIONES O REPARACIONES)", "INGERIR ALCOHOL EN LA VIA PUBLICA", "OTROS (ESPECIFICAR EN EL ACAPITE DATOS IMPORTANTES DEL FORMATO DE OCURRENCIAS)"] } ]
                    },
                    { title: "04 AYUDA Y APOYO A PERSONAS Y ENTIDADES", sub_groups: [
                            { title: "0401 AYUDA, AUXILIO Y RESCATE DE PERSONAS", rows: ["RESCATE Y AUXILIO DE PERSONAS", "AUXILIO VIAL", "MENORES Y ANCIANOS EN ESTADO DE ABANDONO", "APOYO AL VECINO", "PERSONAS EXTRAVIADAS, DESAPARECIDAS Y DESORIENTADAS", "APOYO AL TURISTA", "ENTREVISTA POR CONFLICTO VECINAL", "OTROS (ESPECIFICAR EN EL ACAPITE DATOS IMPORTANTES DEL FORMATO DE OCURRENCIAS)"] },
                            { title: "0402 APOYO A OTRAS GERENCIAS O AREAS  DE LA MUNICIPALIDAD", rows: ["FISCALIZACIÓN Y OTROS (ESPECIFICAR EN EL ACAPITE DATOS IMPORTANTES DEL FORMATO DE OCURRENCIAS)"] },
                            { title: "0403 APOYO A OTRAS ENTIDADES", rows: ["POLICÍA NACIONAL Y OTROS  (ESPECIFICAR EN EL ACAPITE DATOS IMPORTANTES DEL FORMATO DE OCURRENCIAS)"] } ]
                    },
                    { title: "05 EMISION DE ALERTAS TEMPRANAS EN DESASTRES, INFRAESTRUCTURA, SERVICIOS Y ESPACIOS PÚBLICOS AFECTADOS Y EN RIESGO", sub_groups: [
                            { title: "0501 DESASTRES", rows: ["HUAYCOS, INUNDACIONES Y DESPLAZAMIENTOS", "OTROS FENOMENOS NATURALES", "INCENDIOS", "AMAGO DE INCENDIOS", "FUGA DE GAS Y DERRAME DE SUSTANCIAS TÓXICAS", "CAÍDA DE PUENTES", "OTROS (ESPECIFICAR EN EL ACAPITE DATOS IMPORTANTES DEL FORMATO DE OCURRENCIAS)"] },
                            { title: "0502 INFRAESTRUCTURA Y SERVICIOS ESENCIALES AFECTADOS", rows: ["CORTE DE FLUIDO ELÉCTRICO / AGUA / INTERNET /GAS / TELEFONO", "ANIEGOS Y/O PROBLEMAS DE DESAGUE", "SEMÁFOROS APAGADOS O CON DESPERFECTOS", "CAÍDA DE POSTES, CABLES, ÁRBOLES U OTROS", "VIVENDAS COLAPSADAS", "BUZONES SIN TAPA", "TRABAJO DE TERCEROS (INFORMAL Y/O SIN CONDICIONES DE SEGURIDAD)", "OTROS"] },
                            { title: "0503 ESPACIOS PÚBLICOS EN RIESGO", rows: ["PARQUES Y CALLES SIN ILUMINACIÓN O DEFICIENTE", "OBRAS INCONCLUSAS EN LA VIA PÙBLICA", "CONSTRUCCIONES ABANDONADAS", "VIAS SEMI OBSTRUIDAS (POR BACHES PROFUNDOS O GRANCONGESTIÓN VEHICULAR)", "MOBILIARIO URBANO DETERIORADO Y/O FALTA DE MANTENIMIENTO", "VENTA DE MERCADERIA USADA (CACHINAS)", "TALLERES EN LA VIA PÙBLICA", "OTROS (ESPECIFICAR EN EL ACAPITE DATOS IMPORTANTES DEL FORMATO DE OCURRENCIAS)"] } ]
                    },
                    { title: "06 ACONTECIMIENTOS ESPECIALES", sub_groups: [
                            { title: "0601 PREUNTOS ACONTECIMIENTOS ESPECIALES", rows: ["SUICIDIO", "INTENTO DE SUICIDIO", "MUERTE REPENTINA Y/O NATURAL (ESPECIFICAR EN EL ACAPITE DATOS IMPORTANTES DEL FORMATO DE OCURRENCIAS)", "OTROS (ESPECIFICAR EN EL ACAPITE DATOS IMPORTANTES DEL FORMATO DE OCURRENCIAS)"] } ]
                    },
                    { title: "07 OPERATIVOS", sub_groups: [
                            { title: "0701 OPERATIVO MUNICIPAL", rows: ["OPERATIVO PREVENTIVO", "PROTECCIÒN ESCOLAR", "ACELERAMIENTO Y DESCONGESTION VEHICULAR", "OTROS OPERATIVOS MUNICIPALES (ESPECIFICAR EN LA SECCIÓN DATOS IMPORTANTES DEL FORMATO DE OCURRENCIAS)"] },
                            { title: "0702 ESTRATEGIAS Y OPERATIVOS ESPECIALES", rows: ["EN APOYO A LA PNP EN PROGRAMAS PREVENTIVOS (ESPECIFICAR EN EL ACAPITE DATOS IMPORTANTES DEL FORMATO DE OCURRENCIAS)", "APOYO A OTRAS ENTIDADES EN PROGRAMAS PREVENTIVOS  (ESPECIFICAR EN EL ACAPITE DATOS IMPORTANTES DEL FORMATO DE OCURRENCIAS)", "SERENAZGO SIN FRONTERAS", "PATRULLAJE MIXTO (SERENAZGO, JUNTAS VECINALES Y PNP)", "CONTACTO Y CONTROL CIUDADANO", "OTROS (ESPECIFICAR EN EL ACAPITE DATOS IMPORTANTES DEL FORMATO DE OCURRENCIAS)"] } ]
                    }
                ]
            },
            'graficos': { type: 'custom', title: 'Gráficos Comparativos', renderer: renderGraficosSection },
            'recursos': {
                title: 'Recursos',
                isParent: true,
                children: {
                    'vehiculos': {
                        title: 'Vehículos',
                        isParent: true,
                        children: {
                            'motos': { type: 'standard', title: 'Motos', collection: 'motos', fields: ['operativas', 'mantenimiento', 'inoperativas', 'patrullajes', 'intervenciones', 'incidencias'], labels: ['Nro. Operativas', 'Nro. en Mantenimiento', 'Nro. Inoperativas', 'Nro. Patrullajes', 'Nro. Intervenciones', 'Nro. Incidencias'] },
                            'camionetas': { type: 'standard', title: 'Camionetas', collection: 'camionetas', fields: ['operativas', 'mantenimiento', 'inoperativas', 'patrullajes', 'intervenciones', 'incidencias'], labels: ['Nro. Operativas', 'Nro. en Mantenimiento', 'Nro. Inoperativas', 'Nro. Patrullajes', 'Nro. Intervenciones', 'Nro. Incidencias'] },
                            'dron': { type: 'standard', title: 'Drones', collection: 'dron', fields: ['sector1', 'sector2', 'sector3', 'sector4', 'sector5'], labels: ['Sector 1', 'Sector 2', 'Sector 3', 'Sector 4', 'Sector 5'] },
                            'bicicletas': { type: 'standard', title: 'Bicicletas', collection: 'bicicletas', fields: ['operativas', 'mantenimiento', 'inoperativas'], labels: ['Nro. Operativas', 'Nro. en Mantenimiento', 'Nro. Inoperativas'] },
                            'scooters': { type: 'standard', title: 'Scooters', collection: 'scooters', fields: ['operativas', 'mantenimiento', 'inoperativas'], labels: ['Nro. Operativas', 'Nro. en Mantenimiento', 'Nro. Inoperativas'] },
                        }
                    },
                    'personal': {
                        title: 'Personal',
                        isParent: true,
                        children: {
                            'personal_serenazgo': { type: 'standard', title: 'Personal Serenazgo', collection: 'personal_serenazgo', fields: ['a_pie', 'motorizado', 'conductor', 'olaya', 'ichma', 'brigada_canina', 'gestores', 'prevencion'], labels: ['A Pie', 'Motorizado', 'Conductor', 'Olaya', 'Ichma', 'Brigada Canina', 'Gestores', 'Prevención'] },
                            'personal_cecop': { type: 'standard', title: 'Personal CECOP', collection: 'personal_cecop', fields: ['total', 'operadores', 'supervisores'], labels: ['Administracion', 'Nro. Operadores', 'Nro. Supervisores'] },
                        }
                    },
                    'infraestructura': {
                        title: 'Infraestructura',
                        isParent: true,
                        children: {
                             'modulos_serenazgo': { type: 'standard', title: 'Módulos de Serenazgo', collection: 'modulos_serenazgo', fields: ['sector1', 'sector2', 'sector3', 'sector4', 'sector5'], labels: ['Sector 1', 'Sector 2', 'Sector 3', 'Sector 4', 'Sector 5'] },
                             'camaras': { type: 'standard', title: 'Cámaras de Videovigilancia', collection: 'camaras', fields: ['chorrillos_centro', 'matellini', 'pumacahua', 'san_genaro', 'cedros_villa'], labels: ['01 Chorrillos Centro', '02 Matellini', '03 Pumacahua', '04 San Genaro', '05 Cedros de Villa'] },
                        }
                    }
                }
            },
            'frustacion_robos': { type: 'standard', title: 'Frustración de Robos', collection: 'frustacion_robos', fields: ['chorrillos_centro', 'matellini', 'pumacahua', 'san_genaro', 'cedros_villa'], labels: ['01 Chorrillos Centro', '02 Matellini', '03 Pumacahua', '04 San Genaro', '05 Cedros de Villa'] },
            'operativos': { type: 'standard', title: 'Operativos y Patrullaje', collection: 'operativos', fields: ['serenazgo', 'escuadron_verde', 'halcones', 'integrado', 'convenio', 'brigada_canina'], labels: ['Operativos Serenazgo', 'Op. Escuadrón Verde', 'Op. con Halcones', 'Patrullaje Integrado', 'Patrullaje por Convenio', 'Brigada Canina'] },
            'hurto_robo': { type: 'standard', title: 'Delitos por Comisaría', collection: 'hurto_robo', fields: ['chorrillos_hurto', 'chorrillos_robo', 'villa_hurto', 'villa_robo', 'mateo_hurto', 'mateo_robo', 'san_genaro_hurto', 'san_genaro_robo'], labels: ['Hurto (Chorrillos)', 'Robo (Chorrillos)', 'Hurto (Villa)', 'Robo (Villa)', 'Hurto (Mateo)', 'Robo (Mateo)', 'Hurto (San Genaro)', 'Robo (San Genaro)'] },
            'sipcop': { type: 'long_table', title: 'Ocurrencias SIPCOP', collection: 'sipcop', groups: [
                    { title: "01 EMISION DE ALERTAS TEMPRANAS EN APOYO A LA PNP , EN ACTIVIDADES PRESUNTAMENTE DELICTIVAS", rows: ["0101 PRESUNTAS ACTIVIDADES CONTRA LA VIDA EL CUERPO Y LA SALUD", "0102 PRESUNTA ACTIVIDAD CONTRA LA LIBERTAD", "0103 PRESUNTAS ACTIVIDADES CONTRA EL PATRIMONIO", "0104 PRESUNTAS ACTIVIDADES CONTRA LA SEG. PUB.", "0105 PRE. ACTIV.CONTRA LA SALUD PUB.", "0106 PRE. ACTIV. AMBIENTALES", "0107 PRESUNTA ACTIVIDAD CONTRA LA TRANQUILIDAD PUB", "0108 PRESUNTAS ACTIV. CONTRA LA ADM. PUB.", "0109 PRE. ACTIV. CONTRA LOS PODERES DEL ESTADO"] },
                    { title: "02 EMISION DE ALERTAS TEMPRANAS EN APOYO A LA PNP EN PRESUNTAS FALTAS", rows: ["0201 PRESUNTA ACTIV. CONTRA CONTRA LA PERSONA", "0202 PRESUNTA ACTIV. CONTRA EL PATRIMONIO", "0203 PRE. ACTIV. CONTRA LA SEG. PÚBLICA", "0204 PRESUNTA ACTIV. CONTRA LAS BUENAS COSTUMBRES", "0205 PRESUNTA ACTIV. CONTRA LA TRANQUIL. PUB."] },
                    { title: "03 EMISION DE ALERTAS TEMPRANAS EN APOYO A LA PNP Y OTRAS GERENCIAS, EN PRESUNTAS INFRACCIONES", rows: ["0301 PRESUNTOS ACCIDENTES E INFRACCIONES AL TRÁNSITO Y TRANSPORTE", "0302 PRESUNTA ACTIV. CONTRA LA LEY DE PROTEC. Y BIENESTAR ANIMAL", "0303 PRESUNTA ACTIV. DE PERSONAS QUE AFECTAN TRANQUILIDAD Y EL ORDEN", "0304 PRESUNTAS INFRACCIONES A LAS ORDENANZAS Y LICENCIAS MUNICIPALES"] },
                    { title: "04 AYUDA Y APOYO A PERSONAS Y ENTIDADES", rows: ["0401 AYUDA, AUXILIO Y RESCATE DE PERSONAS", "0402 APOYO A OTRAS GERENCIAS O AREAS DE LA MUNICIPALIDAD", "0403 APOYO A OTRAS ENTIDADES"] },
                    { title: "05 EMISION DE ALERTAS TEMPRANAS EN DESASTRES, INFRAESTRUCTURA, SERVICIOS Y ESPACIOS PÚBLICOS AFECTADOS Y EN RIESGO", rows: ["0501 DESASTRES", "0502 INFRAESTRUCTURA Y SERVICIOS ESENCIALES AFECTADOS", "0503 ESPACIOS PÚBLICOS EN RIESGO"] },
                    { title: "06 ACONTECIMIENTOS ESPECIALES", rows: ["0601 PRESUNTOS ACONTECIMIENTOS ESPECIALES"] },
                    { title: "07 OPERATIVOS", rows: ["0701 OPERATIVO MUNICIPAL", "0702 ESTRATEGIAS Y OPERATIVOS ESPECIALES"] }
                ] },
            'observatorio': { type: 'long_table', title: 'Observatorio del Delito', collection: 'observatorio', rows: ['Administración pública (delito)', 'Adolescente infractor de la ley penal', 'Contravención a los derechos de los niños y adolescentes', 'Derechos intelectuales (delito)', 'Faltas', 'Familia (delito)', 'Fe publica (delito)', 'Honor (delito)', 'Humanidad (delito)', 'Ley 30096 delitos informáticos, modificada por la ley 30171', 'Ley de violencia contra la mujer y los integrantes del grupo familiar'] },
            'incidencias': { type: 'long_table', title: 'Incidencias', collection: 'incidencias', groups: [
                    { title: '0203 PRE. ACTIV. CONTRA LA SEG. PÚBLICA', rows: ['ARROJO DE BASURA O QUEMARLA CAUSANDO MOLESTIAS','PRESUNTA OBSTRUCCIÓN Y ARROJO DE ESCOMBROS Y/O MATERIALES. TAMBIÉN NO COLOCAR SEÑALES PREVENTIVAS EN LA VÍA'] },
                    { title: '0301 PRESUNTOS ACCIDENTES E INFRACCIONES AL TRÁNSITO Y TRANSPORTE', rows: ['PRESUNTO ATROPELLO','PRESUNTO ATROPELLO Y FUGA','PRESUNTO CHOQUE','PRESUNTO CHOQUE Y FUGA','OTROS PRESUNTOS ACCIDENTES (DESPISTE, VOLCADURA, CAIDA DE PASAJERO E INCENDIO)','PRESUNTO VEHÍCULO ABANDONADO','VEHÍCULOS NO AUTORIZADOS O PROHIBIDOS PARA SERVICIO PÚBLICO','VEHÍCULOS QUE HACEN PIQUES Y CARRERAS.','PARADEROS Y VEHÍCULOS DE TRANSPORTE PÚBLICO INFORMALES','VEHÍCULOS ESTACIONADOS EN ZONA PROHIBIDA','OTRAS INFRACCIONES AL RGTO. DE TRÁNSITO.'] },
                    { title: '0303 PRESUNTA ACTIV. DE PERSONAS QUE AFECTAN TRANQUILIDAD Y EL ORDEN', rows: ['LLAMADORES, JALADORES Y PARQUEADORES INFORMALES (QUE GRITAN Y SE APROPIAN DE ESPACIOS PÚBLICOS)','RECICLADORES FORMALES O INFORMALES','ORATES Y/O INDIGENTES (AGRESIVOS, QUE EXIGEN DINERO O SE POSESIONAN DE ESPACIOS, INTERRUMPIENDO ACTIVIDADES PÚBLICAS O PRIVADAS)','PRESUNTAS PERSONAS EN ACTITUD SOSPECHOSA','PRESUNTAS PERSONAS EN VEHÍCULOS SOSPECHOSOS','PRESUNTOS TRABAJADORES SEXUALES EN LA VIA PÙBLICA (MASCULINO O FEMENINO)','PRESUNTAS PERSONAS QUE MICCIONAN EN VIA PÚBLICA','OTROS PRESUNTOS (ESPECIFICAR EN LA SECCIÓN DATOS IMPORTANTES DEL FORMATO DE OCURRENCIAS)'] },
                    { title: '0304 PRESUNTAS INFRACCIONES A LAS ORDENANZAS Y LICENCIAS MUNICIPALES', rows: ['COMERCIO EN VÍA PÚBLICA DISTINTA AL AUTORIZADO','COMERCIO EN VÍA PÚBLICA NO AUTORIZADO','CONSTRUCCIONES SIN LICENCIA MUNICIPAL','RUIDOS MOLESTOS','INGERIR ALCOHOL EN LA VIA PÚBLICA'] },
                    { title: '0401 AYUDA, AUXILIO Y RESCATE DE PERSONAS', rows: ['RESCATE Y AUXILIO DE PERSONAS','AUXILIO VIAL','MENORES Y ANCIANOS EN ESTADO DE ABANDONO','APOYO AL VECINO','PERSONAS EXTRAVIADAS, DESAPARECIDAS Y DESORIENTADAS','APOYO AL TURISTA','ENTREVISTA POR CONFLICTO VECINAL'] },
                    { title: '0402 APOYO A OTRAS GERENCIAS O AREAS DE LA MUNICIPALIDAD', rows: ['FISCALIZACIÓN Y OTROS (ESPECIFICAR EN LA SECCIÓN DATOS IMPORTANTES DEL FORMATO DE OCURRENCIAS)'] },
                    { title: '0502 INFRAESTRUCTURA Y SERVICIOS ESENCIALES AFECTADOS', rows: ['SEMÁFOROS APAGADOS O CON DESPERFECTOS','CAÍDA DE POSTES, CABLES, ÁRBOLES U OTROS','BUZONES SIN TAPA'] },
                    { title: '0503 ESPACIOS PÚBLICOS EN RIESGO', rows: ['PARQUES Y CALLES SIN ILUMINACIÓN O DEFICIENTE','OBRAS INCONCLUSAS EN LA VIA PÚBLICA','CONSTRUCCIONES ABANDONadas','TALLERES EN LA VIA PÚBLICA'] }
                ]
            },
            'lima_metropolitana': { type: 'long_table', title: 'Intervenciones (Lima Metro)', collection: 'lima_metropolitana', groups: [
                    { title: '0102 PRESUNTA ACTIVIDAD CONTRA LA LIBERTAD', rows: ['PRESUNTA TRATA DE PERSONAS'] },
                    { title: '0103 PRESUNTAS ACTIVIDADES CONTRA EL PATRIMONIO', rows: ['PRESUNTO ROBO A PERSONAS (CELULARES, MOCHILAS, CARTERAS, BILLETERAS, RELOJES, JOYAS Y OTROS)','PRESUNTO ROBO DE CASA HABITADA','PRESUNTO ROBO DE VEHÍCULOS (MAYORES Y MENORES)','PRESUNTO ROBO DE ACCESORIOS Y AUTOPARTES','PRESUNTO ROBO A PASAJEROS EN VEHÍCULOS DE TRANSPORTE Y MERCANCIAS EN VEHÍCULOS DE CARGA Y REPARTIDORES','PRESUNTO DAÑO (DAÑAR, DESTRUIR O INUTILIZAR BIEN MUEBLE O INMUEBLE)','PRESUNTO HURTO A PERSONAS (CELULARES, MOCHILAS, CARTERAS, BILLETERAS, RELOJES, JOYAS Y OTROS)','HURTO DE CASA HABITADA','PRESUNTO HURTO DE VEHÍCULOS (MAYORES Y MENORES)','PRESUNTO HURTO A PASAJEROS EN VEHÍCULOS DE TRANSPORTE Y MERCANCÍAS EN VEHÍCULOS DE CARGA Y REPARTIDORES','PRESUNTO OTROS (RECEPTACION, APROPIACION ILICITA, ESTAFA, EXTORSION, CHANTAJE Y USURPACIÓN DE INMUEBLE)'] },
                    { title: '0201 PRESUNTA ACTIV. CONTRA LA PERSONA', rows: ['PRESUNTAS LESIONES (MUY LEVES)','PRESUNTO MALTRATO (SIN LESIÓN)','PRESUNTAS PELEAS CALLEJERAS CON LESIONES MUY LEVES O SIN LESIONES'] },
                    { title: '0202 PRESUNTA ACTIV. CONTRA EL PATRIMONIO', rows: ['PRESUNTO DAÑO A LA PROPIEDAD MUEBLE O INMUEBLE','PRESUNTO HURTO FAMÉLICO (CONSUMIR Y NO PAGAR)','PRESUNTA USURPACION BREVE Y JUEGOS PROHIBIDOS'] },
                    { title: '0204 PRESUNTA ACTIV. CONTRA LAS BUENAS COSTUMBRES', rows: ['PRESUNTO CONSUMO DE ALCOHOL O DROGAS PERTURBANDO LA TRANQUILIDAD O SEGURIDAD DE LAS PERSONAS O SUMINISTRARLO A MENORES DE EDAD','PRESUNTA DESTRUCCIÓN DE PLANTAS','PRESUNTO ACTOS DE CRUELDAD CONTRA LOS ANIMALES','PRESUNTA PROPOSICIÓN INMORAL O DESHONESTA'] },
                    { title: '0403 APOYO A OTRAS ENTIDADES', rows: ['POLICÍA NACIONAL Y OTROS (ESPECIFICAR EN LA SECCIÓN DATOS IMPORTANTES DEL FORMATO DE OCURRENCIAS)'] },
                    { title: '0701 OPERATIVO MUNICIPAL', rows: ['OPERATIVO PREVENTIVO','PROTECCIÓN ESCOLAR','OTROS OPERATIVOS MUNICIPALES (ESPECIFICAR EN LA SECCIÓN DATOS IMPORTANTES DEL FORMATO DE OCURRENCIAS)'] },
                    { title: '0702 ESTRATEGIAS Y OPERATIVOS ESPECIALES', rows: ['EN APOYO A LA PNP EN PROGRAMAS PREVENTIVOS (ESPECIFICAR LA SECCIÓN DATOS IMPORTANTES DEL FORMATO DE OCURRENCIAS)','APOYO A OTRAS ENTIDADES EN PROGRAMAS PREVENTIVOS (ESPECIFICAR EN LA SECCIÓN DATOS IMPORTANTES DEL FORMATO DE OCURRENCIAS)','SERENAZGO SIN FRONTERAS','PATRULLAJE MIXTO (SERENAZGO, JUNTAS VECINALES Y PNP)','CONTACTO Y CONTROL CIUDADANO','OTROS (ESPECIFICAR EN LA SECCIÓN DATOS IMPORTANTES DEL FORMATO DE OCURRENCIAS)'] }
                ]
            }
        };
        
        function renderApp(user) {
            renderNavMenu();
            updateAuthUI(user);
            setupEventListeners();
            // Load initial content area
            document.getElementById('content-area').innerHTML = `<div class="text-center p-8 bg-white rounded-lg shadow-md"><h1 class="text-3xl font-bold text-[var(--color-primary)]">Bienvenido al Sistema de Estadísticas</h1><p class="mt-4 text-gray-600">Selecciona una categoría del menú para comenzar.</p></div>`;
        }

        function updateAuthUI(user) {
            const authContainer = document.getElementById('auth-container');
            if (user && !user.isAnonymous) {
                // User is signed in
                authContainer.innerHTML = `
                    <div class="text-center">
                        <p class="text-sm text-white mb-2">Autorizado: ${user.displayName || user.email}</p>
                        <button id="logout-btn" class="w-full text-sm py-2 px-4 rounded-md bg-[var(--color-accent2)] hover:bg-[var(--color-accent3)] transition-colors">Cerrar Sesión</button>
                    </div>
                `;
            } else {
                // User is anonymous
                authContainer.innerHTML = `
                    <div class="text-center">
                        <button id="login-btn" class="w-full text-sm py-2 px-4 rounded-md bg-[var(--color-secondary)] hover:bg-[#9ab1d1] transition-colors">Iniciar Sesión Autorizada</button>
                    </div>
                `;
            }
        }

        function renderNavMenu() {
            const navMenu = document.getElementById('nav-menu');
            const menuHtml = Object.entries(sections).map(([key, section]) => {
                if (section.authorizedOnly && !isAuthorized) return '';
                if (section.isParent) {
                    return `
                        <details class="nav-group">
                            <summary class="nav-link-parent">${section.title}</summary>
                            <div class="pl-4">
                                ${Object.entries(section.children).map(([childKey, childSection]) => {
                                    if (childSection.isParent) {
                                        return `
                                            <details class="nav-group">
                                                <summary class="nav-link-parent pl-4">${childSection.title}</summary>
                                                <div class="pl-8">
                                                    ${Object.entries(childSection.children).map(([grandChildKey, grandChildSection]) => `
                                                        <a href="#" data-section="${grandChildKey}" class="nav-link block py-2 px-4 rounded transition duration-200">${grandChildSection.title}</a>
                                                    `).join('')}
                                                </div>
                                            </details>
                                        `;
                                    } else {
                                        return `<a href="#" data-section="${childKey}" class="nav-link block py-2 px-4 rounded transition duration-200">${childSection.title}</a>`;
                                    }
                                }).join('')}
                            </div>
                        </details>
                    `;
                } else {
                    return `<a href="#" data-section="${key}" class="nav-link block py-2.5 px-4 rounded transition duration-200">${section.title}</a>`;
                }
            }).join('');
            navMenu.innerHTML = menuHtml;
        }

        function findSection(sectionId) {
            for (const key in sections) {
                if (key === sectionId) return sections[key];
                if (sections[key].isParent) {
                    for (const childKey in sections[key].children) {
                        if (childKey === sectionId) return sections[key].children[childKey];
                        if (sections[key].children[childKey].isParent) {
                            for (const grandChildKey in sections[key].children[childKey].children) {
                                if (grandChildKey === sectionId) return sections[key].children[childKey].children[grandChildKey];
                            }
                        }
                    }
                }
            }
            return null;
        }

        function setupEventListeners() {
            document.getElementById('nav-menu').addEventListener('click', e => {
                // Hacemos el selector más específico para apuntar solo a los enlaces de sección.
                const navLink = e.target.closest('a.nav-link[data-section]');
                if (navLink) {
                    e.preventDefault(); 
                    renderSection(navLink.dataset.section); 
                    // En pantallas móviles, cerramos la barra lateral después del clic.
                    if (window.innerWidth < 768) {
                        document.getElementById('sidebar').classList.add('-translate-x-full');
                    }
                }
            });
            document.getElementById('menu-toggle').addEventListener('click', () => document.getElementById('sidebar').classList.toggle('-translate-x-full'));
            
            document.getElementById('auth-container').addEventListener('click', (e) => {
                if (e.target.id === 'login-btn') {
                    signInWithPopup(auth, googleProvider).catch(error => console.error("Login failed:", error));
                }
                if (e.target.id === 'logout-btn') {
                    signOut(auth).catch(error => console.error("Logout failed:", error));
                }
            });
        }

        function unsubscribeAllListeners() { dbListeners.forEach(unsub => unsub()); dbListeners = []; }

        async function renderSection(sectionId) {
            if (!userId) return;
            unsubscribeAllListeners();
            const section = findSection(sectionId);
            if (!section) { console.error(`Section not found: ${sectionId}`); return; }

            const contentArea = document.getElementById('content-area');

            if (section.type === 'standard') renderStandardSection(contentArea, sectionId, section); 
            else if (section.type === 'long_table') renderLongTableSection(contentArea, sectionId, section); 
            else if (section.type === 'custom') section.renderer(contentArea, sectionId, section);

            if (section.collection) {
                const collectionPath = `artifacts/${appId}/users/${userId}/${section.collection}`;
                const unsubscribe = onSnapshot(collection(db, collectionPath), (snapshot) => {
                    currentData[section.collection] = {};
                    snapshot.forEach(doc => { currentData[section.collection][doc.id] = doc.data(); });
                    
                    if (document.getElementById(`section-container-${sectionId}`)) {
                        if (section.type === 'standard') updateStandardTable(sectionId, currentData[section.collection]);
                        else if (section.type === 'long_table') updateLongTable(sectionId, currentData[section.collection]);
                    }
                });
                dbListeners.push(unsubscribe);
            }
        }
        
        async function handleFormSubmit(e, sectionId, section, allRows) {
            e.preventDefault();
            const form = e.target;
            const monthIndex = form.querySelector('#month-select').value;
            const monthName = months[parseInt(monthIndex, 10)].toLowerCase();
            const dataToSave = {};
            allRows.forEach(row => {
                const rowKey = slugify(row);
                const value = form.querySelector(`#${rowKey}`).value;
                dataToSave[rowKey] = value ? parseInt(value, 10) : 0;
            });

            if (isAuthorized) {
                try {
                    const docRef = doc(db, `artifacts/${appId}/users/${userId}/${section.collection}`, monthName);
                    await setDoc(docRef, dataToSave, { merge: true });
                    alert('¡Datos guardados con éxito!');
                } catch (error) {
                    console.error("Error saving document: ", error);
                    alert('Error al guardar los datos.');
                }
            } else {
                try {
                    const pendingRef = doc(collection(db, `pending_changes`));
                    await setDoc(pendingRef, {
                        sectionId: sectionId,
                        sectionTitle: section.title,
                        collection: section.collection,
                        month: monthName,
                        year: new Date().getFullYear(),
                        submittedBy: userId,
                        submittedByEmail: userEmail || 'Anonymous',
                        submittedAt: serverTimestamp(),
                        data: dataToSave
                    });
                    alert('¡Datos enviados para aprobación!');
                    form.reset();
                } catch (error) {
                    console.error("Error submitting for approval: ", error);
                    alert('Error al enviar los datos para aprobación.');
                }
            }
        }

        async function uploadSipcopData() {
            if (!isAuthorized) {
                alert('No tienes autorización para realizar esta acción.');
                return;
            }
        
            const batch = writeBatch(db);
            const monthKeys = ["enero", "febrero", "marzo", "abril", "mayo", "junio", "julio", "agosto"];
        
            monthKeys.forEach((monthKey, index) => {
                const monthName = months[index].toLowerCase();
                const monthData = {};
                estadisticas.forEach(item => {
                    const rowText = `${item.codigo} ${item.subtipo}`;
                    const rowKey = slugify(rowText);
                    if (item[monthKey] !== undefined) {
                        monthData[rowKey] = item[monthKey];
                    }
                });
                const docRef = doc(db, `artifacts/${appId}/users/${userId}/sipcop`, monthName);
                batch.set(docRef, monthData, { merge: true });
            });
        
            try {
                await batch.commit();
                alert('¡Datos históricos de SIPCOP cargados con éxito!');
            } catch (error) {
                console.error("Error al cargar datos históricos: ", error);
                alert('Error al cargar los datos históricos.');
            }
        }

        async function uploadObservatorioData() {
            if (!isAuthorized) {
                alert('No tienes autorización para realizar esta acción.');
                return;
            }
        
            const batch = writeBatch(db);
            const monthKeys = ["enero", "febrero", "marzo", "abril", "mayo", "junio", "julio", "agosto"];
        
            monthKeys.forEach((monthKey, index) => {
                const monthName = months[index].toLowerCase();
                const monthData = {};
                estadisticasDelitos.forEach(item => {
                    const rowText = item.tipo;
                    const rowKey = slugify(rowText);
                    if (item[monthKey] !== undefined) {
                        monthData[rowKey] = item[monthKey];
                    }
                });
                const docRef = doc(db, `artifacts/${appId}/users/${userId}/observatorio`, monthName);
                batch.set(docRef, monthData, { merge: true });
            });
        
            try {
                await batch.commit();
                alert('¡Datos históricos de Observatorio cargados con éxito!');
            } catch (error) {
                console.error("Error al cargar datos históricos de observatorio: ", error);
                alert('Error al cargar los datos históricos de observatorio.');
            }
        }

        async function uploadDelitosComisariaData() {
            if (!isAuthorized) {
                alert('No tienes autorización para realizar esta acción.');
                return;
            }
        
            const batch = writeBatch(db);
            const section = findSection('hurto_robo');

            delitosComisariaData.forEach(data => {
                const monthName = data.mes;
                const monthData = {};
                section.fields.forEach(field => {
                    if (data[field] !== undefined) {
                        monthData[field] = data[field];
                    }
                });
                const docRef = doc(db, `artifacts/${appId}/users/${userId}/hurto_robo`, monthName);
                batch.set(docRef, monthData, { merge: true });
            });
        
            try {
                await batch.commit();
                alert('¡Datos de Delitos por Comisaría cargados con éxito!');
            } catch (error) {
                console.error("Error al cargar datos de delitos por comisaría: ", error);
                alert('Error al cargar los datos de delitos por comisaría.');
            }
        }

        async function uploadOperativosData() {
            if (!isAuthorized) {
                alert('No tienes autorización para realizar esta acción.');
                return;
            }
        
            const batch = writeBatch(db);
            const section = findSection('operativos');
        
            operativosData.forEach(data => {
                const monthName = data.mes;
                const monthData = {};
                section.fields.forEach(field => {
                    if (data[field] !== undefined) {
                        monthData[field] = data[field];
                    }
                });
                const docRef = doc(db, `artifacts/${appId}/users/${userId}/operativos`, monthName);
                batch.set(docRef, monthData, { merge: true });
            });
        
            try {
                await batch.commit();
                alert('¡Datos de Operativos y Patrullaje cargados con éxito!');
            } catch (error) {
                console.error("Error al cargar datos de operativos: ", error);
                alert('Error al cargar los datos de operativos.');
            }
        }

        // --- SECTION RENDERERS -- -
        function renderStandardSection(container, sectionId, section) {
            const bulkLoadButtonHtml = '';

            container.innerHTML = `
                <div id="section-container-${sectionId}" class="bg-[var(--color-surface)] p-6 rounded-lg shadow-xl">
                    <h2 class="text-2xl font-bold text-[var(--color-primary)] mb-4">${section.title}</h2>
                    <form id="data-form-${sectionId}" class="mb-6 grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4 items-end">
                        <div>
                            <label for="month-select" class="block text-sm font-medium text-[var(--color-text-secondary)]">Mes</label>
                            <select id="month-select" class="mt-1 block w-full pl-3 pr-10 py-2 text-base border-gray-300 focus:outline-none focus:ring-[var(--color-accent2)] focus:border-[var(--color-accent2)] sm:text-sm rounded-md">
                                ${months.map((month, index) => `<option value="${index}">${month}</option>`).join('')}
                            </select>
                        </div>
                        <div class="col-span-1 md:col-span-2 lg:col-span-3 grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-4">
                            ${section.fields.map((field, index) => `<div><label for="${slugify(field)}" class="block text-sm font-medium text-[var(--color-text-secondary)]">${section.labels[index]}</label><input type="number" id="${slugify(field)}" name="${slugify(field)}" class="mt-1 block w-full shadow-sm sm:text-sm border-gray-300 rounded-md focus:ring-[var(--color-accent2)] focus:border-[var(--color-accent2)]"></div>`).join('')}
                        </div>
                        <div class="md:col-start-1 flex items-center">
                            <button type="submit" class="w-full justify-center py-2 px-4 border border-transparent shadow-sm text-sm font-medium rounded-md text-white bg-[var(--color-accent2)] hover:bg-[var(--color-accent3)] focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-[var(--color-accent3)]">Guardar Datos</button>
                            ${bulkLoadButtonHtml}
                        </div>
                    </form>
                    <div class="table-container"><table class="min-w-full divide-y divide-gray-200"><thead class="bg-gray-50 sticky-header"><tr><th class="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Mes</th>${section.labels.map(label => `<th class="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">${label}</th>`).join('')}<th class="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Total</th></tr></thead><tbody id="data-table-body-${sectionId}" class="bg-white divide-y divide-gray-200"></tbody></table></div>
                </div>`;
            
            const form = document.getElementById(`data-form-${sectionId}`);
            form.addEventListener('submit', (e) => handleFormSubmit(e, sectionId, section, section.fields));

            
            form.querySelector('#month-select').addEventListener('change', e => { const monthIndex = e.target.value; const monthName = months[parseInt(monthIndex, 10)].toLowerCase(); const data = currentData[section.collection]?.[monthName] || {}; section.fields.forEach(field => { form.querySelector(`#${slugify(field)}`).value = data[field] || ''; }); });
            form.querySelector('#month-select').dispatchEvent(new Event('change'));
        }

        function updateStandardTable(sectionId, data) {
            const section = findSection(sectionId);
            const tableBody = document.getElementById(`data-table-body-${sectionId}`);
            if (!tableBody) return;
            tableBody.innerHTML = months.map(month => {
                const monthKey = month.toLowerCase(); const monthData = data?.[monthKey]; const values = monthData ? section.fields.map(field => monthData[slugify(field)] ?? 0) : Array(section.fields.length).fill(null); const total = monthData ? values.reduce((sum, val) => sum + (val || 0), 0) : null;
                return `<tr><td class="px-6 py-4 whitespace-nowrap text-sm font-medium text-gray-900">${month}</td>${values.map(val => `<td class="px-6 py-4 whitespace-nowrap text-sm text-gray-500">${val !== null ? val : '-'}</td>`).join('')}${!['motos', 'camionetas', 'bicicletas', 'scooters'].includes(sectionId) ? `<td class="px-6 py-4 whitespace-nowrap text-sm font-semibold text-gray-800">${total !== null ? total : '-'}</td>` : ''}</tr>`;
            }).join('');
        }

        function renderLongTableSection(container, sectionId, section) {
            let formInputsHtml = '';
            const isNested = section.groups && section.groups[0].sub_groups;

            if (isNested) {
                formInputsHtml = section.groups.map(group => `
                    <details class="bg-[#f5f3f6] rounded-lg p-3 mb-3">
                        <summary class="font-bold text-[var(--color-primary)] cursor-pointer">${group.title}</summary>
                        <div class="pl-4 mt-2">
                        ${group.sub_groups.map(sub_group => `
                            <details class="bg-[#faf9fb] rounded-lg p-4 mb-4">
                                <summary class="font-semibold text-[var(--color-secondary)] cursor-pointer">${sub_group.title}</summary>
                                <div class="grid grid-cols-1 md:grid-cols-2 gap-x-6 gap-y-4 mt-4">
                                    ${sub_group.rows.map(row => `
                                        <div>
                                            <label for="${slugify(row)}" class="block text-sm font-medium text-[var(--color-text-secondary)]">${row}</label>
                                            <input type="number" id="${slugify(row)}" name="${slugify(row)}" class="mt-1 block w-full shadow-sm sm:text-sm border-gray-300 rounded-md focus:ring-[var(--color-accent2)] focus:border-[var(--color-accent2)]">
                                        </div>
                                    `).join('')}
                                </div>
                            </details>
                        `).join('')}
                        </div>
                    </details>
                `).join('');
            } else if (section.groups) {
                formInputsHtml = section.groups.map(group => `
                    <details class="bg-[#faf9fb] rounded-lg p-4 mb-4">
                        <summary class="font-semibold text-[var(--color-secondary)] cursor-pointer">${group.title}</summary>
                        <div class="grid grid-cols-1 md:grid-cols-2 gap-x-6 gap-y-4 mt-4">
                            ${group.rows.map(row => `
                                <div>
                                    <label for="${slugify(row)}" class="block text-sm font-medium text-[var(--color-text-secondary)]">${row}</label>
                                    <input type="number" id="${slugify(row)}" name="${slugify(row)}" class="mt-1 block w-full shadow-sm sm:text-sm border-gray-300 rounded-md focus:ring-[var(--color-accent2)] focus:border-[var(--color-accent2)]">
                                </div>
                            `).join('')}
                        </div>
                    </details>
                `).join('');
            } else {
                 formInputsHtml = `<div class="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-x-6 gap-y-4 pt-4">
                            ${section.rows.map(row => `
                                <div>
                                    <label for="${slugify(row)}" class="block text-sm font-medium text-[var(--color-text-secondary)]">${row}</label>
                                    <input type="number" id="${slugify(row)}" name="${slugify(row)}" class="mt-1 block w-full shadow-sm sm:text-sm border-gray-300 rounded-md focus:ring-[var(--color-accent2)] focus:border-[var(--color-accent2)]">
                                </div>
                            `).join('')}
                        </div>`;
            }

            const saveButtonContainer = `
                <div>
                    <button type="submit" class="w-full md:w-auto justify-center py-2 px-6 border border-transparent shadow-sm text-sm font-medium rounded-md text-white bg-[var(--color-accent2)] hover:bg-[var(--color-accent3)] focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-[var(--color-accent3)]">Guardar Datos del Mes</button>
                </div>
            `;

            container.innerHTML = `
                <div id="section-container-${sectionId}" class="bg-[var(--color-surface)] p-6 rounded-lg shadow-xl">
                    <h2 class="text-2xl font-bold text-[var(--color-primary)] mb-4">${section.title}</h2>
                    <form id="data-form-${sectionId}" class="mb-6 space-y-6">
                        <div>
                            <label for="month-select" class="block text-sm font-medium text-[var(--color-text-secondary)]">Mes</label>
                            <select id="month-select" class="mt-1 block w-full md:w-1/3 pl-3 pr-10 py-2 text-base border-gray-300 focus:outline-none focus:ring-[var(--color-accent2)] focus:border-[var(--color-accent2)] sm:text-sm rounded-md">
                                ${months.map((month, index) => `<option value="${index}">${month}</option>`).join('')}
                            </select>
                        </div>
                        <div id="form-inputs" class="space-y-4">${formInputsHtml}</div>
                        ${saveButtonContainer}
                    </form>
                    <div class="table-container"><table class="min-w-full divide-y divide-gray-200"><thead class="bg-gray-50 sticky-header"><tr><th class="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Tipo de Ocurrencia</th>${months.map(m => `<th class="px-3 py-3 text-center text-xs font-medium text-gray-500 uppercase tracking-wider">${m.substring(0,3)}</th>`).join('')}<th class="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Total</th></tr></thead><tbody id="data-table-body-${sectionId}" class="bg-white divide-y divide-gray-200"></tbody></table></div>
                </div>`;

            const form = document.getElementById(`data-form-${sectionId}`);
            const allRows = section.groups ? section.groups.flatMap(g => g.sub_groups ? g.sub_groups.flatMap(sg => sg.rows) : g.rows) : section.rows;
            form.addEventListener('submit', (e) => handleFormSubmit(e, sectionId, section, allRows));

            const populateForm = () => {
                const monthIndex = form.querySelector('#month-select').value;
                const monthName = months[parseInt(monthIndex, 10)].toLowerCase();
                const monthData = currentData[section.collection]?.[monthName] || {};
                allRows.forEach(row => { const input = form.querySelector(`#${slugify(row)}`); if (input) input.value = monthData[slugify(row)] || ''; });
            };

            form.querySelector('#month-select').addEventListener('change', populateForm);
            populateForm();
        }
        
        function updateLongTable(sectionId, data) {
            const section = findSection(sectionId);
            const tableBody = document.getElementById(`data-table-body-${sectionId}`);
            if (!tableBody) return;
            let tableHtml = '';
            const isNested = section.groups && section.groups[0].sub_groups;

            if (isNested) {
                section.groups.forEach(group => {
                    tableHtml += `<tr class="bg-[#e0e7ff]"><td colspan="${months.length + 2}" class="px-4 py-3 text-left text-md font-bold text-[var(--color-primary)]">${group.title}</td></tr>`;
                    group.sub_groups.forEach(sub_group => {
                        tableHtml += `<tr class="bg-[#eef2ff]"><td colspan="${months.length + 2}" class="pl-8 pr-4 py-2 text-left text-sm font-semibold text-[var(--color-secondary)]">${sub_group.title}</td></tr>`;
                        sub_group.rows.forEach(rowText => { tableHtml += generateTableRow(rowText, data); });
                    });
                });
            } else if (section.groups) {
                section.groups.forEach(group => {
                    tableHtml += `<tr class="bg-[#eef2ff]"><td colspan="${months.length + 2}" class="px-6 py-3 text-left text-sm font-semibold text-[var(--color-secondary)]">${group.title}</td></tr>`;
                    group.rows.forEach(rowText => { tableHtml += generateTableRow(rowText, data); });
                });
            } else {
                section.rows.forEach(rowText => { tableHtml += generateTableRow(rowText, data); });
            }
            tableBody.innerHTML = tableHtml;
        }

                function generateTableRow(rowText, data) {
            const rowKey = slugify(rowText); let total = 0;
            const monthCells = months.map(month => { const monthKey = month.toLowerCase(); const value = data?.[monthKey]?.[rowKey] ?? 0; total += value; return `<td class="px-3 py-4 whitespace-nowrap text-sm text-center text-gray-500">${value || '-'}</td>`; }).join('');
            return `<tr><td class="pl-12 pr-6 py-4 whitespace-normal text-sm font-medium text-gray-900">${rowText}</td>${monthCells}<td class="px-6 py-4 whitespace-nowrap text-sm font-semibold text-gray-800">${total || '-'}</td></tr>`;
        }

        async function renderGraficosSection(container, sectionId, section) {
            container.innerHTML = `
            <div id="section-container-${sectionId}" class="bg-[var(--color-surface)] p-6 rounded-lg shadow-xl">
                <h2 class="text-2xl font-bold text-[var(--color-primary)] mb-6">${section.title}</h2>
                <div class="grid grid-cols-1 lg:grid-cols-2 gap-8">
                    <div class="bg-[#faf9fb] p-4 rounded-lg shadow-md">
                        <h3 class="text-lg font-semibold text-[var(--color-secondary)] mb-4">Intervenciones vs. Incidencias</h3>
                        <canvas id="chart-intervenciones-incidencias"></canvas>
                    </div>
                    <div class="bg-[#faf9fb] p-4 rounded-lg shadow-md">
                        <h3 class="text-lg font-semibold text-[var(--color-secondary)] mb-4">Evolución de Robos a Personas</h3>
                        <canvas id="chart-robos-personas"></canvas>
                    </div>
                    <div class="bg-[#faf9fb] p-4 rounded-lg shadow-md">
                        <h3 class="text-lg font-semibold text-[var(--color-secondary)] mb-4">Delitos por Comisaría (Último Mes)</h3>
                        <canvas id="chart-delitos-comisaria"></canvas>
                    </div>
                    <div class="bg-[#faf9fb] p-4 rounded-lg shadow-md">
                        <h3 class="text-lg font-semibold text-[var(--color-secondary)] mb-4">Tipos de Operativos (Último Mes)</h3>
                        <canvas id="chart-tipos-operativos"></canvas>
                    </div>
                </div>
            </div>`;
            await fetchAllDataForCharts();
        }

        async function fetchAllDataForCharts() {
            const allCollections = Object.values(sections).flatMap(s => s.isParent ? Object.values(s.children).flatMap(c => c.isParent ? Object.values(c.children) : c) : s).map(s => s.collection).filter(Boolean);
            const uniqueCollections = [...new Set(allCollections)];
            for (const collectionName of uniqueCollections) {
                const collectionPath = `artifacts/${appId}/users/${userId}/${collectionName}`;
                const querySnapshot = await getDocs(collection(db, collectionPath));
                currentData[collectionName] = {};
                querySnapshot.forEach(doc => { currentData[collectionName][doc.id] = doc.data(); });
            }
            updateCharts();
        }

        function updateCharts() {
            if (!document.getElementById('chart-intervenciones-incidencias')) return;
            Object.values(charts).forEach(chart => chart.destroy());
            const chartColors = ['#3b5994', '#7c94b4', '#caa7b5', '#f1888b', '#f16f74', '#ec3538', '#8e44ad', '#2ecc71'];

            // Chart 1: Intervenciones vs Incidencias
            const motosData = currentData.motos || {};
            const intervenciones = months.map(m => motosData[m.toLowerCase()]?.intervenciones || 0);
            const incidenciasData = currentData.incidencias || {};
            const totalIncidencias = months.map(m => {
                const monthData = incidenciasData[m.toLowerCase()] || {};
                return Object.values(monthData).reduce((a, b) => a + b, 0);
            });
            charts.intervencionesIncidencias = new Chart(document.getElementById('chart-intervenciones-incidencias').getContext('2d'), {
                type: 'bar',
                data: { labels: months, datasets: [
                        { label: 'Intervenciones', data: intervenciones, backgroundColor: '#7c94b4' },
                        { label: 'Incidencias', data: totalIncidencias, backgroundColor: '#f1888b' }
                    ]
                }
            });

            // Chart 2: Robos a Personas
            const limaMetroData = currentData.lima_metropolitana || {};
            const robosKey = slugify('PRESUNTO ROBO A PERSONAS (CELULARES, MOCHILAS, CARTERAS, BILLETERAS, RELOJES, JOYAS Y OTROS)');
            const robosData = months.map(m => limaMetroData[m.toLowerCase()]?.[robosKey] || 0);
            charts.robosPersonas = new Chart(document.getElementById('chart-robos-personas').getContext('2d'), {
                type: 'line',
                data: { labels: months, datasets: [{ label: 'Presuntos Robos a Personas', data: robosData, borderColor: '#3b5994', backgroundColor: 'rgba(59, 89, 148, 0.1)', fill: true, tension: 0.1 }] }
            });

            // Chart 3: Delitos por Comisaría
            const hurtoRoboData = currentData.hurto_robo || {};
            const lastMonthWithData = [...months].reverse().find(m => hurtoRoboData[m.toLowerCase()] && Object.keys(hurtoRoboData[m.toLowerCase()]).length > 0);
            const lastMonthData = lastMonthWithData ? hurtoRoboData[lastMonthWithData.toLowerCase()] : {};
            const hurtoRoboLabels = findSection('hurto_robo').labels;
            const hurtoRoboValues = findSection('hurto_robo').fields.map(f => lastMonthData[f] || 0);
            charts.delitosComisaria = new Chart(document.getElementById('chart-delitos-comisaria').getContext('2d'), {
                type: 'doughnut',
                data: { labels: hurtoRoboLabels, datasets: [{ label: 'Delitos', data: hurtoRoboValues, backgroundColor: chartColors }] },
                options: { responsive: true, plugins: { legend: { position: 'top' } } }
            });

            // Chart 4: Tipos de Operativos
            const operativosData = currentData.operativos || {};
            const lastMonthOperativos = [...months].reverse().find(m => operativosData[m.toLowerCase()] && Object.keys(operativosData[m.toLowerCase()]).length > 0);
            const lastMonthOperativosData = lastMonthOperativos ? operativosData[lastMonthOperativos.toLowerCase()] : {};
            const operativosLabels = findSection('operativos').labels;
            const operativosValues = findSection('operativos').fields.map(f => lastMonthOperativosData[f] || 0);
            charts.tiposOperativos = new Chart(document.getElementById('chart-tipos-operativos').getContext('2d'), {
                type: 'bar',
                data: { labels: operativosLabels, datasets: [{ label: 'Nro. de Operativos', data: operativosValues, backgroundColor: chartColors }] },
                options: { indexAxis: 'y', responsive: true, plugins: { legend: { display: false } } }
            });
        }
        
        function renderAprobacionesSection(container, sectionId, section) {
            container.innerHTML = `
            <div id="section-container-${sectionId}" class="bg-[var(--color-surface)] p-6 rounded-lg shadow-xl">
                <h2 class="text-2xl font-bold text-[var(--color-primary)] mb-6">${section.title}</h2>
                <div id="aprobaciones-list" class="space-y-4"></div>
            </div>`;

            const listContainer = document.getElementById('aprobaciones-list');

            const unsubscribe = onSnapshot(collection(db, "pending_changes"), (snapshot) => {
                if (snapshot.empty) {
                    listContainer.innerHTML = `<p class="text-gray-500">No hay cambios pendientes de aprobación.</p>`;
                    return;
                }
                listContainer.innerHTML = snapshot.docs.map(doc => {
                    const data = doc.data();
                    const date = data.submittedAt?.toDate().toLocaleString('es-PE') || 'Fecha no disponible';
                    return `
                        <div class="bg-white p-4 rounded-lg shadow-md border border-gray-200">
                            <div class="flex justify-between items-center">
                                <div>
                                    <p class="font-bold text-lg text-gray-800">${data.sectionTitle} - ${data.month.charAt(0).toUpperCase() + data.month.slice(1)} ${data.year}</p>
                                    <p class="text-sm text-gray-500">Enviado por: ${data.submittedByEmail} el ${date}</p>
                                </div>
                                <div>
                                    <button data-id="${doc.id}" class="approve-btn text-white bg-green-500 hover:bg-green-600 px-4 py-2 rounded-md mr-2">Aprobar</button>
                                    <button data-id="${doc.id}" class="reject-btn text-white bg-red-500 hover:bg-red-600 px-4 py-2 rounded-md">Rechazar</button>
                                </div>
                            </div>
                        </div>
                    `;
                }).join('');
            });
            dbListeners.push(unsubscribe);

            listContainer.addEventListener('click', async (e) => {
                const target = e.target;
                const id = target.dataset.id;
                if (!id) return;

                if (target.classList.contains('approve-btn')) {
                    try {
                        const pendingDocRef = doc(db, 'pending_changes', id);
                        const pendingDocSnap = await getDoc(pendingDocRef);
                        if (!pendingDocSnap.exists()) {
                            alert("Este cambio ya no existe.");
                            return;
                        }
                        const change = pendingDocSnap.data();
                        // NOTE: This uses the currently logged-in admin's UID to write the data, not the original submitter's.
                        const finalDocRef = doc(db, `artifacts/${appId}/users/${userId}/${change.collection}`, change.month);
                        
                        const batch = writeBatch(db);
                        batch.set(finalDocRef, change.data, { merge: true });
                        batch.delete(pendingDocRef);
                        await batch.commit();

                        alert("Cambio aprobado y publicado.");
                    } catch (error) {
                        console.error("Error approving change: ", error);
                        alert("Error al aprobar el cambio.");
                    }
                }

                if (target.classList.contains('reject-btn')) {
                    try {
                        await deleteDoc(doc(db, 'pending_changes', id));
                        alert("Cambio rechazado.");
                    } catch (error) {
                        console.error("Error rejecting change: ", error);
                        alert("Error al rechazar el cambio.");
                    }
                }
            });
        }

        // --- BULK UPLOAD MODAL ---
        function initializeBulkUploadModal() {
            const bulkUploadBtn = document.getElementById('bulk-upload-btn');
            const bulkUploadModal = document.getElementById('bulk-upload-modal');
            const cancelBtn = document.getElementById('cancel-btn');
            const uploadBtn = document.getElementById('upload-btn');
            const sectionSelect = document.getElementById('section-select-modal');

            // Populate section select
            const allSections = [];
            const getSections = (sectionsObject) => {
                Object.entries(sectionsObject).forEach(([key, section]) => {
                    if (section.isParent) {
                        getSections(section.children);
                    } else {
                        if (section.type === 'standard' || section.type === 'long_table') {
                            allSections.push({ key, title: section.title });
                        }
                    }
                });
            };
            getSections(sections);

            sectionSelect.innerHTML = allSections.map(s => `<option value="${s.key}">${s.title}</option>`).join('');

            bulkUploadBtn.addEventListener('click', () => {
                if (isAuthorized) {
                    bulkUploadModal.classList.remove('hidden');
                } else {
                    alert('No tienes autorización para realizar una carga masiva.');
                }
            });

            cancelBtn.addEventListener('click', () => {
                bulkUploadModal.classList.add('hidden');
            });

            uploadBtn.addEventListener('click', handleFileUpload);
        }

        async function handleFileUpload() {
            const sectionKey = document.getElementById('section-select-modal').value;
            const file = document.getElementById('excel-file').files[0];
            const section = findSection(sectionKey);

            if (!file || !section) {
                alert('Por favor, selecciona una sección y un archivo.');
                return;
            }

            const reader = new FileReader();
            reader.onload = async (e) => {
                try {
                    const data = new Uint8Array(e.target.result);
                    const workbook = XLSX.read(data, { type: 'array' });
                    const sheetName = workbook.SheetNames[0];
                    const worksheet = workbook.Sheets[sheetName];
                    const jsonData = XLSX.utils.sheet_to_json(worksheet);

                    if (jsonData.length === 0) {
                        alert('El archivo Excel está vacío o no tiene el formato correcto.');
                        return;
                    }

                    if (section.type === 'standard') {
                        await processStandardBulkUpload(section, jsonData);
                    } else if (section.type === 'long_table') {
                        await processLongTableBulkUpload(section, jsonData);
                    }

                    alert('¡Carga masiva completada con éxito!');
                    document.getElementById('bulk-upload-modal').classList.add('hidden');
                    document.getElementById('excel-file').value = ''; // Reset file input
                } catch (error) {
                    console.error("Error procesando el archivo:", error);
                    alert('Hubo un error al procesar el archivo. Revisa la consola para más detalles.');
                }
            };
            reader.readAsArrayBuffer(file);
        }

        async function processStandardBulkUpload(section, jsonData) {
            if (!isAuthorized) {
                alert('No tienes autorización para realizar esta acción.');
                return;
            }

            const batch = writeBatch(db);
            const labelToFieldMap = new Map(section.labels.map((label, index) => [label, section.fields[index]]));

            jsonData.forEach(row => {
                const monthName = row['Mes']?.toLowerCase();
                if (!monthName || !months.map(m => m.toLowerCase()).includes(monthName)) {
                    console.warn(`Fila ignorada: mes inválido o ausente: ${row['Mes']}`, row);
                    return;
                }

                const dataToSave = {};
                for (const [label, value] of Object.entries(row)) {
                    if (label === 'Mes') continue;
                    const fieldKey = labelToFieldMap.get(label);
                    if (fieldKey) {
                        dataToSave[fieldKey] = !isNaN(parseInt(value, 10)) ? parseInt(value, 10) : 0;
                    } else {
                        console.warn(`Columna ignorada en la sección "${section.title}": el encabezado "${label}" no coincide con ninguna etiqueta definida.`);
                    }
                }

                if (Object.keys(dataToSave).length > 0) {
                    const docRef = doc(db, `artifacts/${appId}/users/${userId}/${section.collection}`, monthName);
                    batch.set(docRef, dataToSave, { merge: true });
                }
            });

            try {
                await batch.commit();
                alert('¡Datos de sección standard cargados con éxito!');
            } catch (error) {
                console.error("Error al cargar datos de sección standard: ", error);
                alert('Error al cargar los datos.');
            }
        }

        async function processLongTableBulkUpload(section, jsonData) {
            if (!isAuthorized) {
                alert('No tienes autorización para realizar esta acción.');
                return;
            }

            const batch = writeBatch(db);
            const allRows = section.groups ? section.groups.flatMap(g => g.sub_groups ? g.sub_groups.flatMap(sg => sg.rows) : g.rows) : section.rows;
            const rowToSlugMap = new Map(allRows.map(row => [row, slugify(row)]));
            
            const dataByMonth = {};

            jsonData.forEach(row => {
                const rowText = row['Tipo de Ocurrencia']; // Assuming this is the header for the row text
                if (!rowText) {
                    console.warn('Fila ignorada: "Tipo de Ocurrencia" ausente.', row);
                    return;
                }
                
                const rowKey = rowToSlugMap.get(rowText);
                if (!rowKey) {
                    console.warn(`Fila ignorada en la sección "${section.title}": la ocurrencia "${rowText}" no es válida.`);
                    return;
                }

                for (const [header, value] of Object.entries(row)) {
                    if (header === 'Tipo de Ocurrencia') continue;
                    
                    const monthName = header.toLowerCase();
                    if (months.map(m => m.toLowerCase()).includes(monthName)) {
                        if (!dataByMonth[monthName]) {
                            dataByMonth[monthName] = {};
                        }
                        dataByMonth[monthName][rowKey] = !isNaN(parseInt(value, 10)) ? parseInt(value, 10) : 0;
                    } else {
                        console.warn(`Columna ignorada en la sección "${section.title}": el encabezado de mes "${header}" no es válido.`);
                    }
                }
            });

            for (const [monthName, data] of Object.entries(dataByMonth)) {
                if (Object.keys(data).length > 0) {
                    const docRef = doc(db, `artifacts/${appId}/users/${userId}/${section.collection}`, monthName);
                    batch.set(docRef, data, { merge: true });
                }
            }

            try {
                await batch.commit();
                alert('¡Datos de sección de tabla larga cargados con éxito!');
            } catch (error) {
                console.error("Error al cargar datos de sección de tabla larga: ", error);
                alert('Error al cargar los datos.');
            }
        }

        // --- INITIALIZE THE APP -- -
        document.addEventListener('DOMContentLoaded', () => {
            onAuthStateChanged(auth, (user) => {
                if (user) {
                    // User is signed in.
                    userId = user.uid;
                    userEmail = user.email;
                    isAuthorized = AUTHORIZED_EMAILS.includes(user.email);
                    renderApp(user);
                    initializeBulkUploadModal();
                } else {
                    // User is signed out. Sign in anonymously.
                    signInAnonymously(auth).catch(err => console.error("Anonymous sign-in failed:", err));
                }
            });
        });