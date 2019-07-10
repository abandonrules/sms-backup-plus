package com.zegoggles.smssync.service;

import android.annotation.SuppressLint;
import android.content.Context;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.database.sqlite.SQLiteException;
import android.support.annotation.NonNull;
import android.support.annotation.Nullable;
import android.util.Log;

import com.zegoggles.smssync.contacts.ContactGroupIds;
import com.zegoggles.smssync.mail.DataType;
import com.zegoggles.smssync.preferences.DataTypePreferences;

import static com.zegoggles.smssync.App.LOCAL_LOGV;
import static com.zegoggles.smssync.App.TAG;

public class BackupItemsFetcher {
    private final Context context;

    private final DataTypePreferences preferences;
    private final BackupQueryBuilder queryBuilder;

    BackupItemsFetcher(@NonNull Context context,
                       @NonNull DataTypePreferences preferences,
                       @NonNull BackupQueryBuilder queryBuilder) {
        if (queryBuilder == null) throw new IllegalArgumentException("queryBuilder cannot be null");

        this.queryBuilder = queryBuilder;
        this.context = context;
        this.preferences = preferences;
    }

    public @NonNull Cursor getItemsForDataType(DataType dataType, ContactGroupIds group, int max) {
        if (LOCAL_LOGV) Log.v(TAG, "getItemsForDataType(type=" + dataType + ", max=" + max + ")");
        switch (dataType) {
            case WHATSAPP:
                return new WhatsAppItemsFetcher(context).getItems(preferences.getMaxSyncedDate(DataType.WHATSAPP), max);
            default:
                return performQuery(queryBuilder.buildQueryForDataType(dataType, group, max));
        }
    }

    /**
     * Gets the most recent timestamp for given datatype.
     * @param dataType the data type
     * @return timestamp
     * @throws SecurityException if app does not hold necessary permissions
     */
    public long getMostRecentTimestamp(DataType dataType) {
        switch (dataType) {
            case WHATSAPP:
                return new WhatsAppItemsFetcher(context).getMostRecentTimestamp();
            default:
                return getMostRecentTimestampForQuery(queryBuilder.buildMostRecentQueryForDataType(dataType));
        }
    }

    private long getMostRecentTimestampForQuery(BackupQueryBuilder.Query query) {
        Cursor cursor = performQuery(query);
        try {
            if (cursor.moveToFirst()) {
                return cursor.getLong(0);
            } else {
                return DataType.Defaults.MAX_SYNCED_DATE;
            }
        } finally {
            cursor.close();
        }
    }

    @SuppressLint("Recycle")
    private @NonNull Cursor performQuery(@Nullable BackupQueryBuilder.Query query) {
        if (query == null) return emptyCursor();
        try {
            final Cursor cursor = context.getContentResolver().query(
                    query.uri,
                    query.projection,
                    query.selection,
                    query.selectionArgs,
                    query.sortOrder
            );
            return cursor == null ? emptyCursor() : cursor;
        } catch (SQLiteException e) {
            Log.w(TAG, "error querying DB", e);
            return emptyCursor();
        } catch (NullPointerException e) {
            Log.w(TAG, "error querying DB", e);
            return emptyCursor();
        }
    }

    static Cursor emptyCursor() {
        return new MatrixCursor(new String[]{});
    }
}
