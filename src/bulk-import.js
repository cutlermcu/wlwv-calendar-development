// src/bulk-import.js
// Bulk Import Module for WLWV Calendar

// Helper function to format dates (import from your main helpers or duplicate here)
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

// Simple CSV parser
function parseCSV(csvText) {
    const lines = csvText.split('\n').filter(line => line.trim());
    if (lines.length === 0) return { headers: [], rows: [] };
    
    // Parse headers
    const headers = lines[0].split(',').map(h => h.trim().toLowerCase());
    
    // Parse rows
    const rows = [];
    for (let i = 1; i < lines.length; i++) {
        const values = [];
        let current = '';
        let inQuotes = false;
        
        for (let j = 0; j < lines[i].length; j++) {
            const char = lines[i][j];
            
            if (char === '"') {
                inQuotes = !inQuotes;
            } else if (char === ',' && !inQuotes) {
                values.push(current.trim());
                current = '';
            } else {
                current += char;
            }
        }
        values.push(current.trim());
        
        // Create object from headers and values
        const row = {};
        headers.forEach((header, index) => {
            row[header] = values[index] || '';
        });
        rows.push(row);
    }
    
    return { headers, rows };
}

// Main bulk import handler
export async function handleBulkMaterialsImport(request, env, corsResponse) {
    try {
        const contentType = request.headers.get('content-type') || '';
        let csvData = '';
        let mode = 'preview'; // default to preview mode
        
        // Parse request based on content type
        if (contentType.includes('multipart/form-data')) {
            const formData = await request.formData();
            const file = formData.get('file');
            mode = formData.get('mode') || 'preview';
            
            if (file) {
                csvData = await file.text();
            }
        } else if (contentType.includes('application/json')) {
            const body = await request.json();
            csvData = body.csvData || body.csv || '';
            mode = body.mode || 'preview';
        } else {
            // Assume raw CSV data
            csvData = await request.text();
            mode = new URL(request.url).searchParams.get('mode') || 'preview';
        }
        
        if (!csvData) {
            return corsResponse({ error: 'No CSV data provided' }, 400);
        }
        
        // Parse CSV
        const { headers, rows } = parseCSV(csvData);
        
        // Validate headers
        const requiredHeaders = ['school', 'date', 'grade_level', 'title', 'link'];
        const missingHeaders = requiredHeaders.filter(h => !headers.includes(h));
        
        if (missingHeaders.length > 0) {
            return corsResponse({
                error: 'Missing required headers',
                missingHeaders,
                foundHeaders: headers,
                requiredHeaders
            }, 400);
        }
        
        // Process each row
        const results = {
            total: rows.length,
            valid: [],
            errors: [],
            duplicates: [],
            preview: mode === 'preview'
        };
        
        // Check for duplicates in existing data
        const duplicateCheckPromises = rows.map(async (row, index) => {
            const rowNum = index + 2; // +2 because row 1 is headers, and arrays are 0-indexed
            
            // Validate row data
            const errors = [];
            
            // Validate school
            if (!row.school) {
                errors.push('School is required');
            } else if (!['wlhs', 'wvhs'].includes(row.school.toLowerCase())) {
                errors.push('School must be wlhs or wvhs');
            }
            
            // Validate date
            let formattedDate = null;
            if (!row.date) {
                errors.push('Date is required');
            } else {
                try {
                    formattedDate = formatDate(row.date);
                } catch (e) {
                    errors.push('Invalid date format');
                }
            }
            
            // Validate grade level
            const gradeLevel = parseInt(row.grade_level);
            if (!row.grade_level) {
                errors.push('Grade level is required');
            } else if (![9, 10, 11, 12].includes(gradeLevel)) {
                errors.push('Grade level must be 9, 10, 11, or 12');
            }
            
            // Validate title and link
            if (!row.title) {
                errors.push('Title is required');
            }
            if (!row.link) {
                errors.push('Link is required');
            }
            
            // If there are validation errors, add to errors array
            if (errors.length > 0) {
                results.errors.push({
                    row: rowNum,
                    data: row,
                    errors
                });
                return;
            }
            
            // Check for duplicates (school + grade_level + link)
            const duplicate = await env.DB.prepare(`
                SELECT id, title, date 
                FROM materials 
                WHERE school = ? AND grade_level = ? AND link = ?
            `).bind(
                row.school.toLowerCase(),
                gradeLevel,
                row.link
            ).first();
            
            if (duplicate) {
                results.duplicates.push({
                    row: rowNum,
                    data: row,
                    existing: {
                        id: duplicate.id,
                        title: duplicate.title,
                        date: duplicate.date
                    }
                });
            } else {
                results.valid.push({
                    row: rowNum,
                    data: {
                        ...row,
                        school: row.school.toLowerCase(),
                        date: formattedDate,
                        grade_level: gradeLevel,
                        description: row.description || '',
                        password: row.password || ''
                    }
                });
            }
        });
        
        await Promise.all(duplicateCheckPromises);
        
        // If preview mode, return the results without inserting
        if (mode === 'preview') {
            return corsResponse({
                mode: 'preview',
                summary: {
                    total: results.total,
                    valid: results.valid.length,
                    errors: results.errors.length,
                    duplicates: results.duplicates.length
                },
                valid: results.valid,
                errors: results.errors,
                duplicates: results.duplicates
            });
        }
        
        // Commit mode - insert valid records
        if (results.valid.length === 0) {
            return corsResponse({
                mode: 'commit',
                message: 'No valid records to import',
                summary: {
                    total: results.total,
                    valid: 0,
                    errors: results.errors.length,
                    duplicates: results.duplicates.length
                },
                errors: results.errors,
                duplicates: results.duplicates
            });
        }
        
        // Generate batch ID
        const batchId = `import_${Date.now()}_${Math.random().toString(36).substr(2, 9)}`;
        
        // Begin transaction-like operation
        const insertedIds = [];
        const insertErrors = [];
        
        for (const item of results.valid) {
            try {
                const result = await env.DB.prepare(`
                    INSERT INTO materials (school, date, grade_level, title, link, description, password, import_batch_id)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                `).bind(
                    item.data.school,
                    item.data.date,
                    item.data.grade_level,
                    item.data.title,
                    item.data.link,
                    item.data.description,
                    item.data.password,
                    batchId
                ).run();
                
                insertedIds.push(result.meta.last_row_id);
            } catch (error) {
                insertErrors.push({
                    row: item.row,
                    data: item.data,
                    error: error.message
                });
            }
        }
        
        // Record the import batch
        await env.DB.prepare(`
            INSERT INTO import_batches (id, total_count, success_count, error_count, duplicate_count, status, summary)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        `).bind(
            batchId,
            results.total,
            insertedIds.length,
            results.errors.length + insertErrors.length,
            results.duplicates.length,
            'completed',
            JSON.stringify({
                schools: [...new Set(results.valid.map(v => v.data.school))],
                grades: [...new Set(results.valid.map(v => v.data.grade_level))],
                dateRange: results.valid.length > 0 ? {
                    min: Math.min(...results.valid.map(v => new Date(v.data.date))),
                    max: Math.max(...results.valid.map(v => new Date(v.data.date)))
                } : null
            })
        ).run();
        
        return corsResponse({
            mode: 'commit',
            batchId,
            message: `Successfully imported ${insertedIds.length} materials`,
            summary: {
                total: results.total,
                imported: insertedIds.length,
                errors: results.errors.length + insertErrors.length,
                duplicates: results.duplicates.length
            },
            imported: insertedIds.length,
            errors: [...results.errors, ...insertErrors],
            duplicates: results.duplicates
        });
        
    } catch (error) {
        console.error('Bulk import error:', error);
        return corsResponse({
            error: 'Bulk import failed',
            message: error.message
        }, 500);
    }
}

// Undo bulk import handler
export async function handleUndoBulkImport(env, batchId, corsResponse) {
    try {
        // Check if batch exists and is recent (within 1 week)
        const batch = await env.DB.prepare(`
            SELECT * FROM import_batches 
            WHERE id = ? 
            AND datetime(imported_at) > datetime('now', '-7 days')
        `).bind(batchId).first();
        
        if (!batch) {
            return corsResponse({
                error: 'Batch not found or too old to undo (>1 week)'
            }, 404);
        }
        
        // Delete materials from this batch
        const result = await env.DB.prepare(`
            DELETE FROM materials WHERE import_batch_id = ?
        `).bind(batchId).run();
        
        // Update batch status
        await env.DB.prepare(`
            UPDATE import_batches 
            SET status = 'undone' 
            WHERE id = ?
        `).bind(batchId).run();
        
        return corsResponse({
            success: true,
            message: `Removed ${result.changes} materials from batch ${batchId}`,
            deletedCount: result.changes,
            batch: {
                id: batch.id,
                importedAt: batch.imported_at,
                totalCount: batch.total_count
            }
        });
        
    } catch (error) {
        console.error('Undo import error:', error);
        return corsResponse({
            error: 'Failed to undo import',
            message: error.message
        }, 500);
    }
}

// Get import history
export async function handleGetImportHistory(env, corsResponse) {
    try {
        const result = await env.DB.prepare(`
            SELECT * FROM import_batches 
            WHERE datetime(imported_at) > datetime('now', '-7 days')
            ORDER BY imported_at DESC
            LIMIT 20
        `).all();
        
        return corsResponse({
            imports: result.results,
            count: result.results.length
        });
        
    } catch (error) {
        console.error('Error fetching import history:', error);
        return corsResponse({
            error: 'Failed to fetch import history',
            message: error.message
        }, 500);
    }
}

// Database schema updates for bulk import
export async function initBulkImportSchema(env) {
    try {
        // Create import_batches table
        await env.DB.prepare(`
            CREATE TABLE IF NOT EXISTS import_batches (
                id TEXT PRIMARY KEY,
                imported_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                imported_by TEXT,
                total_count INTEGER NOT NULL,
                success_count INTEGER NOT NULL,
                error_count INTEGER NOT NULL,
                duplicate_count INTEGER NOT NULL,
                status TEXT NOT NULL,
                summary TEXT
            )
        `).run();
        
        // Add import_batch_id column to materials table if it doesn't exist
        try {
            await env.DB.prepare(`
                ALTER TABLE materials ADD COLUMN import_batch_id TEXT
            `).run();
            console.log('Added import_batch_id column to materials table');
        } catch (e) {
            // Column might already exist
            console.log('import_batch_id column may already exist');
        }
        
        // Create indexes for better performance
        await env.DB.prepare(`
            CREATE INDEX IF NOT EXISTS idx_materials_school_grade_link 
            ON materials(school, grade_level, link)
        `).run();
        
        await env.DB.prepare(`
            CREATE INDEX IF NOT EXISTS idx_materials_import_batch 
            ON materials(import_batch_id)
        `).run();
        
        // Create events_import_batches table
        await env.DB.prepare(`
        CREATE TABLE IF NOT EXISTS events_import_batches (
        id TEXT PRIMARY KEY,
        imported_at DATETIME DEFAULT CURRENT_TIMESTAMP,
        imported_by TEXT,
        total_count INTEGER NOT NULL,
        success_count INTEGER NOT NULL,
        error_count INTEGER NOT NULL,
        duplicate_count INTEGER NOT NULL,
        status TEXT NOT NULL,
        summary TEXT
            )
        `).run();

        // Add import_batch_id column to events table if it doesn't exist
        try {
            await env.DB.prepare(`
                ALTER TABLE events ADD COLUMN import_batch_id TEXT
            `).run();
            console.log('Added import_batch_id column to events table');
        } catch (e) {
            // Column might already exist
            console.log('import_batch_id column may already exist for events');
        }

        // Create indexes for better performance
        await env.DB.prepare(`
            CREATE INDEX IF NOT EXISTS idx_events_school_date_title 
            ON events(school, date, title)
        `).run();

        await env.DB.prepare(`
            CREATE INDEX IF NOT EXISTS idx_events_import_batch 
            ON events(import_batch_id)
        `).run();

        console.log('Bulk import schema initialized successfully');
        return true;
        
    } catch (error) {
        console.error('Error initializing bulk import schema:', error);
        throw error;
    }
}

// EVENTS BULK IMPORT FUNCTIONS

// Validate a single event row
function validateEventRow(row, rowIndex) {
    const errors = [];
    
    // Required fields
    if (!row.school || !['wlhs', 'wvhs'].includes(row.school.toLowerCase())) {
        errors.push(`Row ${rowIndex}: School must be 'wlhs' or 'wvhs'`);
    }
    
    if (!row.date) {
        errors.push(`Row ${rowIndex}: Date is required`);
    } else {
        try {
            const date = new Date(row.date);
            if (isNaN(date.getTime())) {
                errors.push(`Row ${rowIndex}: Invalid date format (use YYYY-MM-DD)`);
            }
        } catch (e) {
            errors.push(`Row ${rowIndex}: Invalid date format`);
        }
    }
    
    if (!row.title || row.title.trim().length === 0) {
        errors.push(`Row ${rowIndex}: Title is required`);
    }
    
    // Optional field validation
    if (row.department && !['ASB', 'Life', 'Athletics', 'Art/Theater', 'Counseling', 'Testing', 'Staff'].includes(row.department)) {
        errors.push(`Row ${rowIndex}: Invalid department (must be ASB, Life, Athletics, Art/Theater, Counseling, Testing, or Staff)`);
    }
    
    return errors;
}

    //Main Bulk Events Import Function
    export async function handleBulkEventsImport(request, env, corsResponse) {
        try {
            const contentType = request.headers.get('content-type') || '';
            let csvData = '';
            let mode = 'preview';
            let requestBody = {}; // Add this to store the full request body
            
            // Parse request based on content type
            if (contentType.includes('multipart/form-data')) {
                const formData = await request.formData();
                const file = formData.get('file');
                mode = formData.get('mode') || 'preview';
                requestBody.duplicateAction = formData.get('duplicateAction') || 'skip';
                
                if (file) {
                    csvData = await file.text();
                }
            } else if (contentType.includes('application/json')) {
                requestBody = await request.json(); // Store the full body
                csvData = requestBody.csvData || requestBody.csv || '';
                mode = requestBody.mode || 'preview';
            } else {
                // Assume raw CSV data
                csvData = await request.text();
                mode = new URL(request.url).searchParams.get('mode') || 'preview';
                requestBody.duplicateAction = 'skip';
            }
        
        if (!csvData) {
            return corsResponse({ error: 'No CSV data provided' }, 400);
        }
        
        // Parse CSV
        const { headers, rows } = parseCSV(csvData);
        
        // Validate headers
        const requiredHeaders = ['school', 'date', 'title'];
        const missingHeaders = requiredHeaders.filter(h => !headers.includes(h));
        if (missingHeaders.length > 0) {
            return corsResponse({ 
                error: 'Missing required headers: ' + missingHeaders.join(', '),
                expected: requiredHeaders,
                found: headers
            }, 400);
        }
        
        // Validate and process rows
        const results = {
            valid: [],
            invalid: [],
            duplicates: []
        };
        
        for (let i = 0; i < rows.length; i++) {
            const row = rows[i];
            const rowIndex = i + 2; // +2 because CSV is 1-indexed and has header row
            
            // Validate row
            const errors = validateEventRow(row, rowIndex);
            
            if (errors.length > 0) {
                results.invalid.push({
                    row: rowIndex,
                    data: row,
                    errors: errors
                });
                continue;
            }
            
            // Check for duplicates in database (same school, date, title)
            const existing = await env.DB.prepare(`
                SELECT id FROM events 
                WHERE school = ? AND date = ? AND title = ?
            `).bind(
                row.school.toLowerCase(), 
                formatDate(row.date), 
                row.title.trim()
            ).first();
            
            if (existing) {
                results.duplicates.push({
                    row: rowIndex,
                    data: row,
                    existingId: existing.id,
                    existingEvent: existing // Include the existing event data
                });
                // Don't continue here - let it also be marked as valid so user can choose
            }
            
            // Add to valid results (even if it's a duplicate)
            results.valid.push({
                row: rowIndex,
                data: {
                    school: row.school.toLowerCase(),
                    date: formatDate(row.date),
                    title: row.title.trim(),
                    department: row.department ? row.department.trim() : null,
                    time: row.time ? row.time.trim() : null,
                    description: row.description ? row.description.trim() : ''
                },
                isDuplicate: !!existing
            });
        }
        
        // If preview mode, return validation results
        if (mode === 'preview') {
            return corsResponse({
                mode: 'preview',
                summary: {
                    total: rows.length,
                    valid: results.valid.length,
                    invalid: results.invalid.length,
                    duplicates: results.duplicates.length
                },
                results: results,
                sampleValid: results.valid.slice(0, 5), // Show first 5 valid rows as preview
                headers: headers
            });
        }
        
        // Commit mode - actually insert the data
        if (mode === 'commit') {
            if (results.valid.length === 0) {
                return corsResponse({ 
                    error: 'No valid events to import',
                    summary: {
                        total: rows.length,
                        valid: 0,
                        invalid: results.invalid.length,
                        duplicates: results.duplicates.length
                    }
                }, 400);
            }
            
            const duplicateAction = requestBody.duplicateAction || 'skip';
            
            // Generate batch ID for tracking - MOVE THIS TO THE TOP
            const batchId = crypto.randomUUID();
            let insertedCount = 0;
            
            // Process based on duplicate action
            for (const validRow of results.valid) {
                if (validRow.isDuplicate) {
                    if (duplicateAction === 'skip') {
                        continue; // Skip this row
                    } else if (duplicateAction === 'replace') {
                        // Update existing event
                        await env.DB.prepare(`
                            UPDATE events 
                            SET title = ?, department = ?, time = ?, description = ?, 
                                updated_at = datetime('now'), import_batch_id = ?
                            WHERE school = ? AND date = ? AND title = ?
                        `).bind(
                            validRow.data.title,
                            validRow.data.department,
                            validRow.data.time,
                            validRow.data.description,
                            batchId, // Now this will work
                            validRow.data.school,
                            validRow.data.date,
                            validRow.data.title
                        ).run();
                        insertedCount++;
                    }
                    // For 'importAll', just insert normally (will create duplicate)
                }
                
                if (!validRow.isDuplicate || duplicateAction === 'importAll') {
                    // Insert new event
                    await env.DB.prepare(`
                        INSERT INTO events (school, date, title, department, time, description, import_batch_id)
                        VALUES (?, ?, ?, ?, ?, ?, ?)
                    `).bind(
                        validRow.data.school,
                        validRow.data.date,
                        validRow.data.title,
                        validRow.data.department,
                        validRow.data.time,
                        validRow.data.description,
                        batchId
                    ).run();
                    insertedCount++;
                }
            }
            
            // Record the import batch
            await env.DB.prepare(`
                INSERT INTO events_import_batches (
                    id, imported_by, total_count, success_count, 
                    error_count, duplicate_count, status, summary
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            `).bind(
                batchId,
                'admin',
                rows.length,
                insertedCount,
                results.invalid.length,
                results.duplicates.length,
                'completed',
                JSON.stringify({
                    totalRows: rows.length,
                    validRows: results.valid.length,
                    invalidRows: results.invalid.length,
                    duplicateRows: results.duplicates.length,
                    insertedCount: insertedCount
                })
            ).run();
            
            return corsResponse({
                success: true,
                mode: 'commit',
                imported: insertedCount,
                batchId: batchId,
                summary: {
                    total: rows.length,
                    valid: results.valid.length,
                    invalid: results.invalid.length,
                    duplicates: results.duplicates.length,
                    inserted: insertedCount
                }
            });
        }
    
    } catch (error) {
    console.error('Events bulk import error:', error);
    return corsResponse({
        error: 'Import failed',
        message: error.message
    }, 500);
        }
    }
    


// Undo events bulk import
export async function handleUndoEventsBulkImport(env, batchId, corsResponse) {
    try {
        // Get batch info
        const batch = await env.DB.prepare(`
            SELECT * FROM events_import_batches 
            WHERE id = ? AND status = 'completed'
            AND datetime(imported_at) > datetime('now', '-7 days')
        `).bind(batchId).first();
        
        if (!batch) {
            return corsResponse({
                error: 'Batch not found or too old to undo (>1 week)'
            }, 404);
        }
        
        // Delete events from this batch
        const result = await env.DB.prepare(`
            DELETE FROM events WHERE import_batch_id = ?
        `).bind(batchId).run();
        
        // Update batch status
        await env.DB.prepare(`
            UPDATE events_import_batches 
            SET status = 'undone' 
            WHERE id = ?
        `).bind(batchId).run();
        
        return corsResponse({
            success: true,
            message: `Removed ${result.changes} events from batch ${batchId}`,
            deletedCount: result.changes,
            batch: {
                id: batch.id,
                importedAt: batch.imported_at,
                totalCount: batch.total_count
            }
        });
        
    } catch (error) {
        console.error('Undo events import error:', error);
        return corsResponse({
            error: 'Failed to undo import',
            message: error.message
        }, 500);
    }
}

// Get events import history
export async function handleGetEventsImportHistory(env, corsResponse) {
    try {
        const result = await env.DB.prepare(`
            SELECT * FROM events_import_batches 
            WHERE datetime(imported_at) > datetime('now', '-7 days')
            ORDER BY imported_at DESC
            LIMIT 20
        `).all();
        
        return corsResponse({
            imports: result.results,
            count: result.results.length
        });
        
    } catch (error) {
        console.error('Error fetching events import history:', error);
        return corsResponse({
            error: 'Failed to fetch import history',
            message: error.message
        }, 500);
    }
}