import { supabase, isSupabaseConfigured } from './supabase';
import { isAbortException } from '../initErrorHandling';
import { MorvelloCloudData } from './firestoreSync';
import { Client } from '../types';

const AGENCY_RECORD_ID = 'morvello_main';

let hasWarnedMissingSchema = false;

/**
 * Generic timeout wrapper to prevent hanging promises (e.g. Supabase web-locks / fetch locks)
 */
function withTimeout<T>(
  promise: Promise<T> | PromiseLike<T>,
  timeoutMs: number,
  timeoutError: string
): Promise<T> {
  return Promise.race([
    Promise.resolve(promise),
    new Promise<T>((_, reject) =>
      setTimeout(() => reject(new Error(timeoutError)), timeoutMs)
    ),
  ]);
}

/**
 * Maps a public.clients database row to the application Client type
 */
export function mapRowToClient(row: any): Client {
  const extraData = row.data && typeof row.data === 'object' ? row.data : {};
  return {
    ...extraData,
    id: row.id,
    firstName: row.first_name || extraData.firstName || '',
    lastName: row.last_name || extraData.lastName || '',
    docType: row.doc_type || extraData.docType || 'CIN',
    docNumber: row.doc_number || extraData.docNumber || '',
    phone: row.phone || extraData.phone || '',
    email: row.email || extraData.email || '',
    contractCount: typeof row.contract_count === 'number' ? row.contract_count : (extraData.contractCount || 0),
    assignedManagerId: row.assigned_manager_id ?? extraData.assignedManagerId ?? undefined,
    createdBy: row.created_by ?? extraData.createdBy ?? undefined,
    createdAt: row.created_at || extraData.createdAt || new Date().toISOString(),
  };
}

/**
 * Fetches clients directly from public.clients table in Supabase PostgreSQL.
 * PostgreSQL Row Level Security (RLS) automatically enforces manager / admin isolation on the database level.
 */
export async function fetchClientsFromSupabase(): Promise<Client[] | null> {
  if (!isSupabaseConfigured) {
    return null;
  }

  try {
    const { data, error } = await withTimeout(
      supabase.from('clients').select('*').order('created_at', { ascending: false }),
      8000,
      'Délai de chargement des clients Supabase dépassé'
    );

    if (error) {
      if (error.code === 'PGRST205' || error.message?.includes('does not exist')) {
        return null;
      }
      console.warn('[Supabase Sync] Error fetching clients:', error.message);
      return null;
    }

    if (!data || !Array.isArray(data)) {
      return [];
    }

    return data.map(mapRowToClient);
  } catch (err: any) {
    if (isAbortException(err)) return null;
    console.warn('[Supabase Sync] Exception fetching clients:', err);
    return null;
  }
}

/**
 * Directly upserts a client in public.clients table
 */
export async function upsertClientInSupabase(client: Client, userId?: string): Promise<boolean> {
  if (!isSupabaseConfigured) {
    return false;
  }

  try {
    const nowIso = new Date().toISOString();
    let authorId = userId;
    if (!authorId) {
      try {
        const { data: sessionData } = await supabase.auth.getSession();
        authorId = sessionData?.session?.user?.id;
      } catch {
        // Ignore session retrieval error
      }
    }

    const assignedMgrId = client.assignedManagerId || null;
    const createdBy = client.createdBy || authorId || null;

    const rowPayload = {
      id: client.id,
      first_name: client.firstName,
      last_name: client.lastName,
      doc_type: client.docType || 'CIN',
      doc_number: client.docNumber,
      phone: client.phone || null,
      email: client.email || null,
      contract_count: client.contractCount || 0,
      assigned_manager_id: assignedMgrId,
      created_by: createdBy,
      data: {
        ...client,
        assignedManagerId: assignedMgrId || client.assignedManagerId,
        createdBy: createdBy || client.createdBy,
      },
      updated_at: nowIso,
    };

    const { error } = await withTimeout(
      supabase.from('clients').upsert(rowPayload, { onConflict: 'id' }),
      8000,
      'Délai de sauvegarde du client dépassé'
    );

    if (error) {
      console.error('[Supabase Sync] Failed to upsert client:', error.message);
      return false;
    }

    return true;
  } catch (err: any) {
    if (isAbortException(err)) return false;
    console.error('[Supabase Sync] Exception during client upsert:', err);
    return false;
  }
}

/**
 * Directly deletes a client from public.clients table
 */
export async function deleteClientFromSupabase(id: string): Promise<boolean> {
  if (!isSupabaseConfigured) {
    return false;
  }

  try {
    const { error } = await withTimeout(
      supabase.from('clients').delete().eq('id', id),
      8000,
      'Délai de suppression du client dépassé'
    );

    if (error) {
      console.error('[Supabase Sync] Failed to delete client:', error.message);
      return false;
    }

    return true;
  } catch (err: any) {
    if (isAbortException(err)) return false;
    console.error('[Supabase Sync] Exception during client deletion:', err);
    return false;
  }
}

/**
 * Real-time subscription listening exclusively to public.clients changes
 */
export function subscribeToClientsFromSupabase(
  onClientsChange: (clients: Client[]) => void,
  onError?: (err: any) => void
): () => void {
  if (!isSupabaseConfigured) {
    return () => {};
  }

  let isDisposed = false;

  const channel = supabase
    .channel('clients_changes')
    .on(
      'postgres_changes',
      {
        event: '*',
        schema: 'public',
        table: 'clients',
      },
      async () => {
        if (isDisposed) return;
        try {
          const freshClients = await fetchClientsFromSupabase();
          if (!isDisposed && freshClients) {
            onClientsChange(freshClients);
          }
        } catch (err) {
          if (onError) onError(err);
        }
      }
    )
    .subscribe((status, err) => {
      if (isDisposed) return;
      if (status === 'CHANNEL_ERROR' || err) {
        if (onError && !isAbortException(err)) {
          onError(err || new Error('Supabase Clients Realtime Channel Error'));
        }
      }
    });

  return () => {
    isDisposed = true;
    supabase.removeChannel(channel).catch(() => {});
  };
}

/**
 * Fetches the centralized Morvello Cars agency state from Supabase PostgreSQL
 */
export async function fetchRemoteAgencyDataFromSupabase(): Promise<MorvelloCloudData | null> {
  if (!isSupabaseConfigured) {
    return null;
  }

  try {
    const { data, error } = await supabase
      .from('agency_data')
      .select('data, updated_at, updated_by')
      .eq('id', AGENCY_RECORD_ID)
      .maybeSingle();

    if (error) {
      // PGRST205 / 42P01 means the table hasn't been created yet in the SQL editor
      if (error.code === 'PGRST205' || error.message?.includes('does not exist')) {
        if (!hasWarnedMissingSchema) {
          console.info(
            '[Supabase] Table "agency_data" non trouvée. Veuillez exécuter le script supabase_schema.sql dans votre SQL Editor Supabase.'
          );
          hasWarnedMissingSchema = true;
        }
        return null;
      }
      console.warn('[Supabase Sync] Error fetching agency data:', error.message);
      return null;
    }

    if (data && data.data && typeof data.data === 'object') {
      const agencyData = { ...(data.data as MorvelloCloudData) };
      // Strip clients: clients are now maintained exclusively in public.clients table
      delete agencyData.clients;
      return {
        ...agencyData,
        updatedAt: data.updated_at || (data.data as any).updatedAt,
        updatedBy: data.updated_by || (data.data as any).updatedBy,
      };
    }

    return null;
  } catch (err: any) {
    if (isAbortException(err)) return null;
    console.warn('[Supabase Sync] Exception during fetch:', err);
    return null;
  }
}

/**
 * Saves or updates the Morvello Cars agency state in Supabase PostgreSQL
 */
export async function saveRemoteAgencyDataToSupabase(
  payload: Partial<MorvelloCloudData>,
  userId?: string
): Promise<boolean> {
  if (!isSupabaseConfigured) {
    return false;
  }

  try {
    const nowIso = new Date().toISOString();
    const cleanPayload = {
      ...payload,
      updatedAt: nowIso,
      updatedBy: userId || 'morvello_user',
    };
    // Strip clients from agency_data blob: clients live exclusively in public.clients table
    delete (cleanPayload as any).clients;

    const { error } = await supabase
      .from('agency_data')
      .upsert(
        {
          id: AGENCY_RECORD_ID,
          data: cleanPayload,
          updated_at: nowIso,
          updated_by: userId || 'system',
        },
        { onConflict: 'id' }
      );

    if (error) {
      if (error.code === 'PGRST205' || error.message?.includes('does not exist')) {
        if (!hasWarnedMissingSchema) {
          console.warn(
            '[Supabase] Exécutez le script supabase_schema.sql dans Supabase pour activer la sauvegarde PostgreSQL.'
          );
          hasWarnedMissingSchema = true;
        }
        return false;
      }
      console.error('[Supabase Sync] Failed to save agency data:', error.message);
      return false;
    }

    // Synchronisation granulaire dans les tables individuelles Supabase (en tâche de fond sécurisée)
    syncIndividualTables(cleanPayload).catch((err) => {
      console.warn('[Supabase Sync] Granular tables sync note:', err);
    });

    return true;
  } catch (err: any) {
    if (isAbortException(err)) return false;
    console.error('[Supabase Sync] Exception during save:', err);
    return false;
  }
}

/**
 * Real-time listener using Supabase Realtime Channels
 */
export function subscribeToRemoteAgencyDataFromSupabase(
  onData: (data: MorvelloCloudData) => void,
  onError?: (err: any) => void
): () => void {
  if (!isSupabaseConfigured) {
    return () => {};
  }

  let isDisposed = false;

  const channel = supabase
    .channel('agency_data_realtime')
    .on(
      'postgres_changes',
      {
        event: '*',
        schema: 'public',
        table: 'agency_data',
        filter: `id=eq.${AGENCY_RECORD_ID}`,
      },
      (payload) => {
        if (isDisposed) return;
        try {
          const newRecord = payload.new as any;
          if (newRecord && newRecord.data && typeof newRecord.data === 'object') {
            const remoteData = { ...(newRecord.data as MorvelloCloudData) };
            // Ensure agency_data realtime events never overwrite clients
            delete remoteData.clients;
            onData({
              ...remoteData,
              updatedAt: newRecord.updated_at,
              updatedBy: newRecord.updated_by,
            });
          }
        } catch (err) {
          console.warn('[Supabase Realtime] Error processing record:', err);
        }
      }
    )
    .subscribe((status, err) => {
      if (isDisposed) return;
      if (status === 'CHANNEL_ERROR' || err) {
        if (onError && !isAbortException(err)) {
          onError(err || new Error('Supabase Realtime Channel Error'));
        }
      }
    });

  return () => {
    isDisposed = true;
    supabase.removeChannel(channel);
  };
}

/**
 * Save user profile in Supabase profiles table
 */
export async function saveUserProfileToSupabase(
  uid: string,
  profile: { role: string; email: string; name?: string; phone?: string; permissions?: any }
): Promise<boolean> {
  if (!isSupabaseConfigured) return false;

  try {
    const { error } = await supabase.from('profiles').upsert(
      {
        id: uid,
        email: profile.email,
        name: profile.name || profile.email.split('@')[0],
        role: profile.role,
        permissions: profile.permissions || {},
        phone: profile.phone || null,
        updated_at: new Date().toISOString(),
      },
      { onConflict: 'id' }
    );

    if (error) {
      if (error.code !== 'PGRST205') {
        console.warn('[Supabase] Could not update profile:', error.message);
      }
      return false;
    }
    return true;
  } catch {
    return false;
  }
}

/**
 * Synchronise les entités individuelles dans leurs tables PostgreSQL dédiées
 */
export async function syncIndividualTables(payload: Partial<MorvelloCloudData>): Promise<void> {
  if (!isSupabaseConfigured) return;

  try {
    // 1. Véhicules
    if (payload.vehicles && Array.isArray(payload.vehicles) && payload.vehicles.length > 0) {
      for (const v of payload.vehicles) {
        const assignedMgrId =
          v.assignedManagerId ||
          (v.assignedManagerName && payload.users?.find((u) => u.name.toLowerCase() === v.assignedManagerName?.toLowerCase())?.id) ||
          null;
        const createdBy = (v as any).createdBy || (v as any).proposedBy || (payload.updatedBy ? String(payload.updatedBy) : null);

        await supabase.from('vehicles').upsert(
          {
            id: v.id,
            brand: v.brand,
            model: v.model,
            plate: v.plate,
            fuel_type: v.fuelType,
            status: v.status,
            current_km: v.currentKm,
            daily_rate: v.dailyRate,
            assigned_manager_id: assignedMgrId,
            created_by: createdBy,
            approval_status: v.approvalStatus || 'approved',
            data: {
              ...v,
              assignedManagerId: assignedMgrId || v.assignedManagerId,
              createdBy: createdBy || (v as any).createdBy,
            },
            updated_at: new Date().toISOString(),
          },
          { onConflict: 'id' }
        );
      }
    }

    // 2. Clients
    if (payload.clients && Array.isArray(payload.clients) && payload.clients.length > 0) {
      for (const c of payload.clients) {
        const assignedMgrId =
          c.assignedManagerId ||
          payload.contracts?.find(
            (cnt) => cnt.clientId === c.id || (cnt.clientSnapshot && cnt.clientSnapshot.id === c.id)
          )?.assignedManagerId ||
          (c.assignedManagerName && payload.users?.find((u) => u.name.toLowerCase() === c.assignedManagerName?.toLowerCase())?.id) ||
          null;
        const createdBy = (c as any).createdBy || (payload.updatedBy ? String(payload.updatedBy) : null);

        await supabase.from('clients').upsert(
          {
            id: c.id,
            first_name: c.firstName,
            last_name: c.lastName,
            doc_type: c.docType,
            doc_number: c.docNumber,
            phone: c.phone,
            email: c.email,
            contract_count: c.contractCount || 0,
            assigned_manager_id: assignedMgrId,
            created_by: createdBy,
            data: {
              ...c,
              assignedManagerId: assignedMgrId || c.assignedManagerId,
              createdBy: createdBy || (c as any).createdBy,
            },
            updated_at: new Date().toISOString(),
          },
          { onConflict: 'id' }
        );
      }
    }

    // 3. Contrats
    if (payload.contracts && Array.isArray(payload.contracts) && payload.contracts.length > 0) {
      for (const cnt of payload.contracts) {
        const assignedMgrId =
          cnt.assignedManagerId ||
          payload.vehicles?.find(
            (v) =>
              v.id === cnt.vehicleId ||
              (cnt.vehicleSnapshot?.plate && v.plate.trim() === cnt.vehicleSnapshot.plate.trim())
          )?.assignedManagerId ||
          (cnt.assignedManagerName && payload.users?.find((u) => u.name.toLowerCase() === cnt.assignedManagerName?.toLowerCase())?.id) ||
          null;
        const createdBy = cnt.createdBy || (payload.updatedBy ? String(payload.updatedBy) : null);

        await supabase.from('contracts').upsert(
          {
            id: cnt.id,
            contract_number: cnt.contractNumber,
            status: cnt.status,
            client_id: cnt.clientId,
            vehicle_id: cnt.vehicleId,
            start_date: cnt.startDate,
            end_date: cnt.endDate,
            total_amount: cnt.totalAmount,
            deposit_amount: cnt.depositAmount,
            assigned_manager_id: assignedMgrId,
            created_by: createdBy,
            data: {
              ...cnt,
              assignedManagerId: assignedMgrId || cnt.assignedManagerId,
              createdBy: createdBy || cnt.createdBy,
            },
            updated_at: new Date().toISOString(),
          },
          { onConflict: 'id' }
        );
      }
    }

    // 4. Cautions
    if (payload.deposits && Array.isArray(payload.deposits) && payload.deposits.length > 0) {
      for (const dep of payload.deposits) {
        const matchedContract = payload.contracts?.find(
          (cnt) => cnt.id === dep.contractId || cnt.contractNumber === dep.contractNumber
        );
        const matchedVehicle = payload.vehicles?.find(
          (v) => dep.vehiclePlate && v.plate.trim() === dep.vehiclePlate.trim()
        );
        const assignedMgrId =
          dep.assignedManagerId ||
          matchedContract?.assignedManagerId ||
          matchedVehicle?.assignedManagerId ||
          (dep.assignedManagerName && payload.users?.find((u) => u.name.toLowerCase() === dep.assignedManagerName?.toLowerCase())?.id) ||
          null;
        const createdBy =
          dep.createdBy ||
          dep.receivedBy ||
          (payload.updatedBy ? String(payload.updatedBy) : null);

        await supabase.from('deposits').upsert(
          {
            id: dep.id,
            contract_id: dep.contractId,
            client_name: dep.clientName,
            amount: dep.amount,
            status: dep.status === 'held' ? 'pending' : dep.status,
            method: dep.method,
            assigned_manager_id: assignedMgrId,
            created_by: createdBy,
            data: {
              ...dep,
              assignedManagerId: assignedMgrId || dep.assignedManagerId,
              createdBy: createdBy || dep.createdBy,
            },
            updated_at: new Date().toISOString(),
          },
          { onConflict: 'id' }
        );
      }
    }
  } catch (syncErr) {
    console.warn('[Supabase Sync] syncIndividualTables caught:', syncErr);
  }
}

