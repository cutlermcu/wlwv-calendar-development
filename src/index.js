import { 
    handleBulkMaterialsImport, 
    handleUndoBulkImport, 
    handleGetImportHistory,
    initBulkImportSchema 
} from './bulk-import.js';

// Helper function to format dates consistently
function formatDate(dateInput) {
    if (!dateInput) return null;

    if (dateInput instanceof Date) {
        return dateInput.toISOString().split('T')[0];
    }

    const date = new Date(dateInput);
    if (isNaN(date.getTime())) {
        throw new Error('Invalid date format');
    }

    return date.toISOString().split('T')[0];
}

// Helper function to handle CORS
function corsHeaders() {
    return {
        'Access-Control-Allow-Origin': '*',
        'Access-Control-Allow-Methods': 'GET, POST, PUT, DELETE, OPTIONS',
        'Access-Control-Allow-Headers': 'Content-Type, Accept',
        'Access-Control-Max-Age': '86400',
    };
}

function corsResponse(response = null, status = 200) {
    const headers = corsHeaders();
    
    if (response) {
        if (typeof response === 'object') {
            headers['Content-Type'] = 'application/json';
            return new Response(JSON.stringify(response), { status, headers });
        }
        return new Response(response, { status, headers });
    }
    
    return new Response(null, { status, headers });
}

export default {
    async fetch(request, env, ctx) {
        // Handle CORS preflight requests
        if (request.method === 'OPTIONS') {
            return corsResponse();
        }

        const url = new URL(request.url);
        
        // API routes
        if (url.pathname.startsWith('/api/')) {
            return handleApiRequest(request, env, url);
        }
        
        // Serve static assets for everything else
        return env.ASSETS.fetch(request);
    }
};

async function handleApiRequest(request, env, url) {
    const method = request.method;
    const pathname = url.pathname;

    try {
        // Root API info
        if (pathname === '/api/' || pathname === '/api') {
            return corsResponse({
                name: 'WLWV Life Calendar API',
                version: '3.0.0',
                status: 'running',
                environment: env.NODE_ENV || 'production',
                database: 'Cloudflare D1',
                endpoints: {
                    health: '/api/health',
                    init: 'POST /api/init',
                    daySchedules: '/api/day-schedules',
                    dayTypes: '/api/day-types',
                    events: '/api/events',
                    materials: '/api/materials',
                    bulkImport: 'POST /api/materials/bulk',
                    importHistory: '/api/materials/imports'
                },
                features: [
                    'Password-protected materials',
                    'Multi-school support',
                    'A/B day scheduling',
                    'Event management',
                    'Grade-level materials',
                    'Bulk import with CSV',
                    'Serverless D1 database'
                ]
            });
        }

        // Health check
        if (pathname === '/api/health') {
            try {
                const result = await env.DB.prepare('SELECT datetime("now") as timestamp').first();
                
                return corsResponse({ 
                    status: 'healthy', 
                    message: 'Database connected',
                    connected: true,
                    timestamp: result?.timestamp,
                    database: 'Cloudflare D1',
                    environment: env.NODE_ENV || 'production'
                });
            } catch (error) {
                console.error('Health check failed:', error);
                return corsResponse({ 
                    error: 'Database connection failed',
                    connected: false,
                    details: error.message,
                    environment: env.NODE_ENV || 'production'
                }, 500);
            }
        }

        // Initialize database
        if (pathname === '/api/init' && method === 'POST') {
            return handleDatabaseInit(env);
        }

        // Day schedules routes
        if (pathname === '/api/day-schedules') {
            if (method === 'GET') {
                return handleGetDaySchedules(env);
            } else if (method === 'POST') {
                return handlePostDaySchedule(request, env);
            }
        }

        // Day types routes
        if (pathname === '/api/day-types') {
            if (method === 'GET') {
                return handleGetDayTypes(env);
            } else if (method === 'POST') {
                return handlePostDayType(request, env);
            }
        }

        // Events routes
        if (pathname === '/api/events') {
            if (method === 'GET') {
                return handleGetEvents(request, env, url);
            } else if (method === 'POST') {
                return handlePostEvent(request, env);
            }
        }

        if (pathname.startsWith('/api/events/')) {
            const eventId = pathname.split('/')[3];
            if (method === 'PUT') {
                return handlePutEvent(request, env, eventId);
            } else if (method === 'DELETE') {
                return handleDeleteEvent(env, eventId);
            }
        }

        // Materials routes
        if (pathname === '/api/materials') {
            if (method === 'GET') {
                return handleGetMaterials(request, env, url);
            } else if (method === 'POST') {
                return handlePostMaterial(request, env);
            }
        }

        // BULK IMPORT ROUTES - Fixed placement
        if (pathname === '/api/materials/bulk' && method === 'POST') {
            return handleBulkMaterialsImport(request, env, corsResponse);
        }
        
        // Undo bulk import
        if (pathname.startsWith('/api/materials/bulk/') && method === 'DELETE') {
            const batchId = pathname.split('/')[4];
            return handleUndoBulkImport(env, batchId, corsResponse);
        }
        
        // Get import history
        if (pathname === '/api/materials/imports' && method === 'GET') {
            return handleGetImportHistory(env, corsResponse);
        }

        // Individual material routes
        if (pathname.startsWith('/api/materials/')) {
            const materialId = pathname.split('/')[3];
            if (method === 'PUT') {
                return handlePutMaterial(request, env, materialId);
            } else if (method === 'DELETE') {
                return handleDeleteMaterial(env, materialId);
            }
        }

        // 404 for unknown API routes
        return corsResponse({ 
            error: 'Not found',
            path: pathname,
            method: method
        }, 404);

    } catch (error) {
        console.error('API Error:', error);
        return corsResponse({ 
            error: 'Internal server error',
            message: error.message
        }, 500);
    }
}

async function handleDatabaseInit(env) {
    try {
        console.log('Initializing database schema...');

        // Create tables using D1
        await env.DB.prepare(`
            CREATE TABLE IF NOT EXISTS day_schedules (
                date TEXT PRIMARY KEY,
                schedule TEXT NOT NULL CHECK (schedule IN ('A', 'B')),
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
            )
        `).run();

        await env.DB.prepare(`
            CREATE TABLE IF NOT EXISTS day_types (
                date TEXT PRIMARY KEY,
                type TEXT NOT NULL,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
            )
        `).run();

        await env.DB.prepare(`
            CREATE TABLE IF NOT EXISTS events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                school TEXT NOT NULL CHECK (school IN ('wlhs', 'wvhs')),
                date TEXT NOT NULL,
                title TEXT NOT NULL,
                department TEXT,
                time TEXT,
                description TEXT,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
            )
        `).run();

        await env.DB.prepare(`
            CREATE TABLE IF NOT EXISTS materials (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                school TEXT NOT NULL CHECK (school IN ('wlhs', 'wvhs')),
                date TEXT NOT NULL,
                grade_level INTEGER NOT NULL CHECK (grade_level BETWEEN 9 AND 12),
                title TEXT NOT NULL,
                link TEXT NOT NULL,
                description TEXT DEFAULT '',
                password TEXT DEFAULT '',
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
            )
        `).run();

        // Create indexes
        await env.DB.prepare(`CREATE INDEX IF NOT EXISTS idx_events_school_date ON events(school, date)`).run();
        await env.DB.prepare(`CREATE INDEX IF NOT EXISTS idx_materials_school_date_grade ON materials(school, date, grade_level)`).run();
        await env.DB.prepare(`CREATE INDEX IF NOT EXISTS idx_events_date ON events(date)`).run();
        await env.DB.prepare(`CREATE INDEX IF NOT EXISTS idx_materials_date ON materials(date)`).run();
        await env.DB.prepare(`CREATE INDEX IF NOT EXISTS idx_day_schedules_date ON day_schedules(date)`).run();
        await env.DB.prepare(`CREATE INDEX IF NOT EXISTS idx_day_types_date ON day_types(date)`).run();

        // Initialize bulk import schema
        await initBulkImportSchema(env);

        console.log('Database schema initialized successfully!');

        return corsResponse({ 
            message: 'Database initialized successfully',
            tables: ['day_schedules', 'day_types', 'events', 'materials', 'import_batches'],
            features: ['password-protected materials', 'multi-school support', 'bulk import', 'performance indexes'],
            database: 'Cloudflare D1',
            environment: env.NODE_ENV || 'production',
            timestamp: new Date().toISOString()
        });

    } catch (error) {
        console.error('Database initialization error:', error);
        return corsResponse({ 
            error: 'Database initialization failed: ' + error.message
        }, 500);
    }
}

async function handleGetDaySchedules(env) {
    try {
        const result = await env.DB.prepare('SELECT date, schedule FROM day_schedules ORDER BY date').all();

        const schedules = result.results.map(row => ({
            date: row.date,
            schedule: row.schedule
        }));

        return corsResponse(schedules);
    } catch (error) {
        console.error('Error fetching day schedules:', error);
        return corsResponse({ error: error.message }, 500);
    }
}

async function handlePostDaySchedule(request, env) {
    try {
        const { date, schedule } = await request.json();

        if (!date) {
            return corsResponse({ error: 'Date is required' }, 400);
        }

        const formattedDate = formatDate(date);

        if (!schedule || schedule === null) {
            await env.DB.prepare('DELETE FROM day_schedules WHERE date = ?').bind(formattedDate).run();
        } else {
            if (!['A', 'B'].includes(schedule)) {
                return corsResponse({ error: 'Schedule must be A or B' }, 400);
            }

            await env.DB.prepare(`
                INSERT OR REPLACE INTO day_schedules (date, schedule, updated_at) 
                VALUES (?, ?, datetime('now'))
            `).bind(formattedDate, schedule).run();
        }

        return corsResponse({ 
            success: true, 
            date: formattedDate, 
            schedule: schedule 
        });
    } catch (error) {
        console.error('Error updating day schedule:', error);
        return corsResponse({ error: error.message }, 500);
    }
}

async function handleGetDayTypes(env) {
    try {
        const result = await env.DB.prepare('SELECT date, type FROM day_types ORDER BY date').all();

        const types = result.results.map(row => ({
            date: row.date,
            type: row.type
        }));

        return corsResponse(types);
    } catch (error) {
        console.error('Error fetching day types:', error);
        return corsResponse({ error: error.message }, 500);
    }
}

async function handlePostDayType(request, env) {
    try {
        const { date, type } = await request.json();

        if (!date) {
            return corsResponse({ error: 'Date is required' }, 400);
        }

        const formattedDate = formatDate(date);

        if (!type || type === null) {
            await env.DB.prepare('DELETE FROM day_types WHERE date = ?').bind(formattedDate).run();
        } else {
            await env.DB.prepare(`
                INSERT OR REPLACE INTO day_types (date, type, updated_at) 
                VALUES (?, ?, datetime('now'))
            `).bind(formattedDate, type).run();
        }

        return corsResponse({ 
            success: true, 
            date: formattedDate, 
            type: type 
        });
    } catch (error) {
        console.error('Error updating day type:', error);
        return corsResponse({ error: error.message }, 500);
    }
}

async function handleGetEvents(request, env, url) {
    try {
        const school = url.searchParams.get('school');

        if (!school) {
            return corsResponse({ error: 'School parameter is required' }, 400);
        }

        if (!['wlhs', 'wvhs'].includes(school)) {
            return corsResponse({ error: 'School must be wlhs or wvhs' }, 400);
        }

        const result = await env.DB.prepare(
            'SELECT id, school, date, title, department, time, description, created_at, updated_at FROM events WHERE school = ? ORDER BY date, time, id'
        ).bind(school).all();

        return corsResponse(result.results);
    } catch (error) {
        console.error('Error fetching events:', error);
        return corsResponse({ error: error.message }, 500);
    }
}

async function handlePostEvent(request, env) {
    try {
        const { school, date, title, department, time, description } = await request.json();

        if (!school || !date || !title) {
            return corsResponse({ error: 'School, date, and title are required' }, 400);
        }

        if (!['wlhs', 'wvhs'].includes(school)) {
            return corsResponse({ error: 'School must be wlhs or wvhs' }, 400);
        }

        const formattedDate = formatDate(date);

        const result = await env.DB.prepare(`
            INSERT INTO events (school, date, title, department, time, description)
            VALUES (?, ?, ?, ?, ?, ?)
        `).bind(school, formattedDate, title, department || null, time || null, description || '').run();

        // Get the inserted record
        const newEvent = await env.DB.prepare(
            'SELECT id, school, date, title, department, time, description, created_at, updated_at FROM events WHERE id = ?'
        ).bind(result.meta.last_row_id).first();

        return corsResponse(newEvent);
    } catch (error) {
        console.error('Error creating event:', error);
        return corsResponse({ error: error.message }, 500);
    }
}

async function handlePutEvent(request, env, eventId) {
    try {
        const { title, department, time, description } = await request.json();

        if (!title) {
            return corsResponse({ error: 'Title is required' }, 400);
        }

        await env.DB.prepare(`
            UPDATE events 
            SET title = ?, department = ?, time = ?, description = ?, updated_at = datetime('now')
            WHERE id = ?
        `).bind(title, department || null, time || null, description || '', eventId).run();

        const updatedEvent = await env.DB.prepare(
            'SELECT id, school, date, title, department, time, description, created_at, updated_at FROM events WHERE id = ?'
        ).bind(eventId).first();

        if (!updatedEvent) {
            return corsResponse({ error: 'Event not found' }, 404);
        }

        return corsResponse(updatedEvent);
    } catch (error) {
        console.error('Error updating event:', error);
        return corsResponse({ error: error.message }, 500);
    }
}

async function handleDeleteEvent(env, eventId) {
    try {
        const result = await env.DB.prepare('DELETE FROM events WHERE id = ?').bind(eventId).run();

        if (result.changes === 0) {
            return corsResponse({ error: 'Event not found' }, 404);
        }

        return corsResponse({ success: true, id: parseInt(eventId) });
    } catch (error) {
        console.error('Error deleting event:', error);
        return corsResponse({ error: error.message }, 500);
    }
}

async function handleGetMaterials(request, env, url) {
    try {
        const school = url.searchParams.get('school');
        
        if (!school) {
            return corsResponse({ error: 'School parameter is required' }, 400);
        }
        
        if (!['wlhs', 'wvhs'].includes(school)) {
            return corsResponse({ error: 'School must be wlhs or wvhs' }, 400);
        }
        
        const result = await env.DB.prepare(
            'SELECT id, school, date, grade_level, title, link, description, password, import_batch_id, created_at, updated_at FROM materials WHERE school = ? ORDER BY date, grade_level, id'
        ).bind(school).all();
        
        return corsResponse(result.results);
    } catch (error) {
        console.error('Error fetching materials:', error);
        return corsResponse({ error: error.message }, 500);
    }
}

async function handlePostMaterial(request, env) {
    try {
        const { school, date, grade_level, title, link, description, password } = await request.json();

        if (!school || !date || !grade_level || !title || !link) {
            return corsResponse({ error: 'School, date, grade_level, title, and link are required' }, 400);
        }

        if (!['wlhs', 'wvhs'].includes(school)) {
            return corsResponse({ error: 'School must be wlhs or wvhs' }, 400);
        }

        if (![9, 10, 11, 12].includes(parseInt(grade_level))) {
            return corsResponse({ error: 'Grade level must be 9, 10, 11, or 12' }, 400);
        }

        const formattedDate = formatDate(date);

        const result = await env.DB.prepare(`
            INSERT INTO materials (school, date, grade_level, title, link, description, password)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        `).bind(school, formattedDate, parseInt(grade_level), title, link, description || '', password || '').run();

        // Get the inserted record
        const newMaterial = await env.DB.prepare(
            'SELECT id, school, date, grade_level, title, link, description, password, created_at, updated_at FROM materials WHERE id = ?'
        ).bind(result.meta.last_row_id).first();

        return corsResponse(newMaterial);
    } catch (error) {
        console.error('Error creating material:', error);
        return corsResponse({ error: error.message }, 500);
    }
}

async function handlePutMaterial(request, env, materialId) {
    try {
        const { title, link, description, password } = await request.json();

        if (!title || !link) {
            return corsResponse({ error: 'Title and link are required' }, 400);
        }

        await env.DB.prepare(`
            UPDATE materials 
            SET title = ?, link = ?, description = ?, password = ?, updated_at = datetime('now')
            WHERE id = ?
        `).bind(title, link, description || '', password || '', materialId).run();

        const updatedMaterial = await env.DB.prepare(
            'SELECT id, school, date, grade_level, title, link, description, password, created_at, updated_at FROM materials WHERE id = ?'
        ).bind(materialId).first();

        if (!updatedMaterial) {
            return corsResponse({ error: 'Material not found' }, 404);
        }

        return corsResponse(updatedMaterial);
    } catch (error) {
        console.error('Error updating material:', error);
        return corsResponse({ error: error.message }, 500);
    }
}

async function handleDeleteMaterial(env, materialId) {
    try {
        const result = await env.DB.prepare('DELETE FROM materials WHERE id = ?').bind(materialId).run();

        if (result.changes === 0) {
            return corsResponse({ error: 'Material not found' }, 404);
        }

        return corsResponse({ success: true, id: parseInt(materialId) });
    } catch (error) {
        console.error('Error deleting material:', error);
        return corsResponse({ error: error.message }, 500);
    }
}