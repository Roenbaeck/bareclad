// used for persistence
use rusqlite::{params, Connection, Error, Statement};
use crate::construct::{Database, Role, Posit, Appearance, AppearanceSet, Thing};
use crate::datatype::{DataType, Decimal, JSON};
use chrono::{NaiveDate};

// ------------- Persistence -------------
pub struct Persistor<'db> {
    pub db: &'db Connection,
    // Adders
    pub add_thing: Statement<'db>,
    pub add_role: Statement<'db>,
    pub add_posit: Statement<'db>,
    // Get the identity of one thing
    pub get_thing: Statement<'db>,
    pub get_role: Statement<'db>,
    pub get_posit: Statement<'db>,
    // Get everything for all things
    pub all_things: Statement<'db>,
    pub all_roles: Statement<'db>,
    pub all_posits: Statement<'db>,
    // DataType particulars
    pub add_data_type: Statement<'db>,
    pub seen_data_types: Vec<u8>,
}
impl<'db> Persistor<'db> {
    pub fn new<'connection>(connection: &'connection Connection) -> Persistor<'connection> {
        // The "STRICT" keyword introduced in 3.37.0 breaks JDBC connections, which makes
        // debugging using an external tool like DBeaver impossible
        connection
            .execute_batch(
                "
            create table if not exists Thing (
                Thing_Identity integer not null, 
                constraint unique_and_referenceable_Thing_Identity primary key (
                    Thing_Identity
                )
            ) STRICT;
            create table if not exists Role (
                Role_Identity integer not null,
                Role text not null,
                Reserved integer not null,
                constraint Role_is_Thing foreign key (
                    Role_Identity
                ) references Thing(Thing_Identity),
                constraint referenceable_Role_Identity primary key (
                    Role_Identity
                ),
                constraint unique_Role unique (
                    Role
                )
            ) STRICT;
            create table if not exists DataType (
                DataType_Identity integer not null,
                DataType text not null,
                constraint referenceable_DataType_Identity primary key (
                    DataType_Identity
                ),
                constraint unique_DataType unique (
                    DataType
                )
            ) STRICT;
            create table if not exists Posit (
                Posit_Identity integer not null,
                AppearanceSet text not null,
                AppearingValue any null, 
                ValueType_Identity integer not null, 
                AppearanceTime any null,
                TimeType_Identity integer not null, 
                constraint Posit_is_Thing foreign key (
                    Posit_Identity
                ) references Thing(Thing_Identity),
                constraint ValueType_is_DataType foreign key (
                    ValueType_Identity
                ) references DataType(DataType_Identity),
                constraint TimeType_is_DataType foreign key (
                    TimeType_Identity
                ) references DataType(DataType_Identity),
                constraint referenceable_Posit_Identity primary key (
                    Posit_Identity
                ),
                constraint unique_Posit unique (
                    AppearanceSet,
                    AppearingValue,
                    AppearanceTime
                )
            ) STRICT;
            ",
            )
            .unwrap();
        Persistor {
            db: connection,
            add_thing: connection
                .prepare(
                    "
                insert into Thing (
                    Thing_Identity
                ) values (?)
            ",
                )
                .unwrap(),
            add_role: connection
                .prepare(
                    "
                insert into Role (
                    Role_Identity, 
                    Role, 
                    Reserved
                ) values (?, ?, ?)
            ",
                )
                .unwrap(),
            add_posit: connection
                .prepare(
                    "
                insert into Posit (
                    Posit_Identity, 
                    AppearanceSet, 
                    AppearingValue, 
                    ValueType_Identity, 
                    AppearanceTime, 
                    TimeType_Identity
                ) values (?, ?, ?, ?, ?, ?)
            ",
                )
                .unwrap(),
            get_thing: connection
                .prepare(
                    "
                select Thing_Identity 
                    from Thing 
                    where Thing_Identity = ?
            ",
                )
                .unwrap(),
            get_role: connection
                .prepare(
                    "
                select Role_Identity 
                    from Role 
                    where Role = ?
            ",
                )
                .unwrap(),
            get_posit: connection
                .prepare(
                    "
                select Posit_Identity 
                    from Posit 
                    where AppearanceSet = ? 
                    and AppearingValue = ? 
                    and AppearanceTime = ?
            ",
                )
                .unwrap(),
            all_things: connection
                .prepare(
                    "
                select Thing_Identity
                    from Thing
            ",
                )
                .unwrap(),
            all_roles: connection
                .prepare(
                    "
                select Role_Identity, Role, Reserved 
                    from Role
            ",
                )
                .unwrap(),
            all_posits: connection
                .prepare(
                    "
                select p.Posit_Identity, 
                        p.AppearanceSet, 
                        p.AppearingValue, 
                        v.DataType as ValueType, 
                        p.AppearanceTime, 
                        t.DataType as TimeType 
                    from Posit p
                    join DataType v
                    on v.DataType_Identity = p.ValueType_Identity
                    join DataType t
                    on t.DataType_Identity = p.TimeType_Identity
            ",
                )
                .unwrap(),
            add_data_type: connection
                .prepare(
                    "
                insert or ignore into DataType (
                    DataType_Identity, 
                    DataType
                ) values (?, ?)
            ",
                )
                .unwrap(),
            seen_data_types: Vec::new(),
        }
    }
    pub fn persist_thing(&mut self, thing: &Thing) -> bool {
        let mut existing = false;
        match self
            .get_thing
            .query_row::<usize, _, _>(params![&thing], |r| r.get(0))
        {
            Ok(_) => {
                existing = true;
            }
            Err(Error::QueryReturnedNoRows) => {
                self.add_thing.execute(params![&thing]).unwrap();
            }
            Err(err) => {
                panic!(
                    "Could not check if the thing '{}' is persisted: {}",
                    &thing, err
                );
            }
        }
        existing
    }
    pub fn persist_role(&mut self, role: &Role) -> bool {
        let mut existing = false;
        match self
            .get_role
            .query_row::<usize, _, _>(params![&role.name()], |r| r.get(0))
        {
            Ok(_) => {
                existing = true;
            }
            Err(Error::QueryReturnedNoRows) => {
                self.add_role
                    .execute(params![&role.role(), &role.name(), &role.reserved()])
                    .unwrap();
            }
            Err(err) => {
                panic!(
                    "Could not check if the role '{}' is persisted: {}",
                    &role.name(),
                    err
                );
            }
        }
        existing
    }
    pub fn persist_posit<V: 'static + DataType, T: 'static + DataType + Ord>(
        &mut self,
        posit: &Posit<V, T>,
    ) -> bool {
        let mut appearances = Vec::new();
        let appearance_set = posit.appearance_set();
        for appearance in appearance_set.appearances().iter() {
            appearances.push(
                appearance.thing().to_string() + "," + &appearance.role().role().to_string(),
            );
        }
        let apperance_set_as_text = appearances.join("|");
        let mut existing = false;
        match self.get_posit.query_row::<usize, _, _>(
            params![&apperance_set_as_text, &posit.value(), &posit.time()],
            |r| r.get(0),
        ) {
            Ok(_) => {
                existing = true;
            }
            Err(Error::QueryReturnedNoRows) => {
                if !self.seen_data_types.contains(&posit.value().identifier()) {
                    self.add_data_type
                        .execute(params![
                            &posit.value().identifier(),
                            &posit.value().data_type()
                        ])
                        .unwrap();
                    self.seen_data_types.push(posit.value().identifier());
                }
                if !self.seen_data_types.contains(&posit.time().identifier()) {
                    self.add_data_type
                        .execute(params![
                            &posit.time().identifier(),
                            &posit.time().data_type()
                        ])
                        .unwrap();
                    self.seen_data_types.push(posit.time().identifier());
                }
                self.add_posit
                    .execute(params![
                        &posit.posit(),
                        &apperance_set_as_text,
                        &posit.value(),
                        &posit.value().identifier(),
                        &posit.time(),
                        &posit.time().identifier()
                    ])
                    .unwrap();
            }
            Err(err) => {
                panic!(
                    "Could not check if the posit {} is persisted: {}",
                    &posit.posit(),
                    err
                );
            }
        }
        existing
    }
    pub fn restore_things(&mut self, db: &Database) {
        let thing_iter = self
        .all_things
        .query_map([], |row| {
            Ok(row.get(0).unwrap())
        })
        .unwrap();
        for thing in thing_iter {
            db.thing_generator().lock().unwrap().retain(thing.unwrap());
        }
    }
    pub fn restore_roles(&mut self, db: &Database) {
        let role_iter = self
            .all_roles
            .query_map([], |row| {
                Ok(Role::new(
                    row.get(0).unwrap(),
                    row.get(1).unwrap(),
                    row.get(2).unwrap(),
                ))
            })
            .unwrap();
        for role in role_iter {
            db.keep_role(role.unwrap());
        }
    }
    pub fn restore_posits(&mut self, db: &Database) {
        let mut rows = self.all_posits.query([]).unwrap();
        while let Some(row) = rows.next().unwrap() {
            let value_type: String = row.get_unwrap(3);
            let time_type: String = row.get_unwrap(5);
            let thing: Thing = row.get_unwrap(0);
            let appearances: String = row.get_unwrap(1);
            let mut appearance_set = Vec::new();
            for appearance_text in appearances.split('|') {
                let (thing, role) = appearance_text.split_once(',').unwrap();
                let appearance = Appearance::new(
                    thing.parse().unwrap(),
                    db
                        .role_keeper()
                        .lock()
                        .unwrap()
                        .lookup(&role.parse::<Thing>().unwrap()),
                );
                let (kept_appearance, _) = db.keep_appearance(appearance);
                appearance_set.push(kept_appearance);
            }
            let (kept_appearance_set, _) = db.keep_appearance_set(AppearanceSet::new(
                appearance_set,
            ).unwrap());

            // MAINTENANCE: The section below needs to be extended when new data types are added
            match (value_type.as_str(), time_type.as_str()) {
                (String::DATA_TYPE, NaiveDate::DATA_TYPE) => {
                    db.keep_posit(
                        Posit::new(
                            thing,
                            kept_appearance_set,
                            <String as DataType>::convert(&row.get_ref_unwrap(2)),
                            <NaiveDate as DataType>::convert(&row.get_ref_unwrap(4))
                        )
                    );
                }, 
                (i64::DATA_TYPE, NaiveDate::DATA_TYPE) => {
                    db.keep_posit(
                        Posit::new(
                            thing,
                            kept_appearance_set,
                            <i64 as DataType>::convert(&row.get_ref_unwrap(2)),
                            <NaiveDate as DataType>::convert(&row.get_ref_unwrap(4))
                        )
                    );
                }, 
                (Decimal::DATA_TYPE, NaiveDate::DATA_TYPE) => {
                    db.keep_posit(
                        Posit::new(
                            thing,
                            kept_appearance_set,
                            <Decimal as DataType>::convert(&row.get_ref_unwrap(2)),
                            <NaiveDate as DataType>::convert(&row.get_ref_unwrap(4))
                        )
                    );
                }, 
                (NaiveDate::DATA_TYPE, NaiveDate::DATA_TYPE) => {
                    db.keep_posit(
                        Posit::new(
                            thing,
                            kept_appearance_set,
                            <NaiveDate as DataType>::convert(&row.get_ref_unwrap(2)),
                            <NaiveDate as DataType>::convert(&row.get_ref_unwrap(4))
                        )
                    );
                }, 
                (JSON::DATA_TYPE, NaiveDate::DATA_TYPE) => {
                    db.keep_posit(
                        Posit::new(
                            thing,
                            kept_appearance_set,
                            <JSON as DataType>::convert(&row.get_ref_unwrap(2)),
                            <NaiveDate as DataType>::convert(&row.get_ref_unwrap(4))
                        )
                    );
                }, 
                (_, _) => ()
            }
        }
    }
}

