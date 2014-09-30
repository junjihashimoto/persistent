{-# LANGUAGE FlexibleContexts, UndecidableInstances #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE EmptyDataDecls #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# OPTIONS_GHC -fno-warn-orphans #-}
module Database.Persist.Zookeeper.Query
       where

import Database.Persist
import Database.Persist.Types
import Database.Persist.Sql
import Control.Applicative (Applicative)
import Data.Monoid
import Data.List
import Control.Monad
import Control.Monad.Base
import Control.Monad.IO.Class (MonadIO (..))
import Control.Monad.Trans.Control (MonadBaseControl)
import Control.Monad.Trans.Class
import qualified Data.Text as T
import qualified Database.Zookeeper as Z
import Database.Persist.Zookeeper.Config
import Database.Persist.Zookeeper.Internal
import Database.Persist.Zookeeper.Store
import Database.Persist.Zookeeper.ZooUtil
import Data.Maybe
import Data.Conduit
import Data.Conduit.Lift



instance (Applicative m, Functor m, MonadIO m, MonadBaseControl IO m) => PersistQuery (ZookeeperT m) where
  update key valList = return ()
  updateGet key valLIst = return $ fromJust $ dummyFromKey key
  updateWhere filterList valList = return ()
  deleteWhere filterList = return ()
  selectSource filterList selectOpts = do
    let dir = entity2path $ dummyFromFList filterList
    (str::[String]) <- lift $ execZookeeperT $ \zk -> do
      Z.getChildren zk dir Nothing
    loop str
    where
      loop [] = return ()
      loop (x:xs) = do
        let key = (txtToKey (T.pack x))
        va <- get key
        case va of
          Nothing ->
            loop xs
          Just v -> do
            yield $ Entity key v
            loop xs
  selectFirst filterList selectOpts = return Nothing
  selectKeys filterList selectOpts = do 
    let dir = entity2path $ dummyFromFList filterList
    (str::[String]) <- lift $ execZookeeperT $ \zk -> do
      Z.getChildren zk dir Nothing
    loop str
    where
      loop [] = return ()
      loop (x:xs) = do
        let key = (txtToKey (T.pack x))
        yield key
        loop xs
  count filterList = return 0
  


updateFieldDef :: PersistEntity v => Update v -> FieldDef SqlType
updateFieldDef (Update f _ _) = persistFieldDef f

dummyFromFilts :: [Filter v] -> Maybe v
dummyFromFilts _ = Nothing

getFiltsValues :: forall val.  PersistEntity val => Database.Persist.Sql.Connection -> [Filter val] -> [PersistValue]
getFiltsValues conn = snd . filterClauseHelper False False conn OrNullNo

data OrNull = OrNullYes | OrNullNo

filterClauseHelper :: PersistEntity val
             => Bool -- ^ include table name?
             -> Bool -- ^ include WHERE?
             -> Database.Persist.Sql.Connection
             -> OrNull
             -> [Filter val]
             -> (T.Text, [PersistValue])
filterClauseHelper includeTable includeWhere conn orNull filters =
    (if not (T.null sql) && includeWhere
        then " WHERE " <> sql
        else sql, vals)
  where
    (sql, vals) = combineAND filters
    combineAND = combine " AND "

    combine s fs =
        (T.intercalate s $ map wrapP a, mconcat b)
      where
        (a, b) = unzip $ map go fs
        wrapP x = T.concat ["(", x, ")"]

    go (BackendFilter _) = error "BackendFilter not expected"
    go (FilterAnd []) = ("1=1", [])
    go (FilterAnd fs) = combineAND fs
    go (FilterOr []) = ("1=0", [])
    go (FilterOr fs)  = combine " OR " fs
    go (Filter field value pfilter) = 
        let t = entityDef $ dummyFromFilts [Filter field value pfilter]
        in case (fieldDB (persistFieldDef field) == DBName "id", entityPrimary t, allVals) of
            -- need to check the id field in a safer way: entityId? 
                 (True, Just pdef, (PersistList ys:_)) -> 
                    if length (primaryFields pdef) /= length ys 
                       then error $ "wrong number of entries in primaryFields vs PersistList allVals=" ++ show allVals
                    else
                      case (allVals, pfilter, isCompFilter pfilter) of
                        ([PersistList xs], Eq, _) -> 
                           let sqlcl=T.intercalate " and " (map (\a -> connEscapeName conn (snd a) <> showSqlFilter pfilter <> "? ")  (primaryFields pdef))
                           in (wrapSql sqlcl,xs)
                        ([PersistList xs], Ne, _) -> 
                           let sqlcl=T.intercalate " or " (map (\a -> connEscapeName conn (snd a) <> showSqlFilter pfilter <> "? ")  (primaryFields pdef))
                           in (wrapSql sqlcl,xs)
                        (_, In, _) -> 
                           let xxs = transpose (map fromPersistList allVals)
                               sqls=map (\(a,xs) -> connEscapeName conn (snd a) <> showSqlFilter pfilter <> "(" <> T.intercalate "," (replicate (length xs) " ?") <> ") ") (zip (primaryFields pdef) xxs)
                           in (wrapSql (T.intercalate " and " (map wrapSql sqls)), concat xxs)
                        (_, NotIn, _) -> 
                           let xxs = transpose (map fromPersistList allVals)
                               sqls=map (\(a,xs) -> connEscapeName conn (snd a) <> showSqlFilter pfilter <> "(" <> T.intercalate "," (replicate (length xs) " ?") <> ") ") (zip (primaryFields pdef) xxs)
                           in (wrapSql (T.intercalate " or " (map wrapSql sqls)), concat xxs)
                        ([PersistList xs], _, True) -> 
                           let zs = tail (inits (primaryFields pdef))
                               sql1 = map (\b -> wrapSql (T.intercalate " and " (map (\(i,a) -> sql2 (i==length b) a) (zip [1..] b)))) zs
                               sql2 islast a = connEscapeName conn (snd a) <> (if islast then showSqlFilter pfilter else showSqlFilter Eq) <> "? "
                               sqlcl = T.intercalate " or " sql1
                           in (wrapSql sqlcl, concat (tail (inits xs)))
                        (_, BackendSpecificFilter _, _) -> error "unhandled type BackendSpecificFilter for composite/non id primary keys"
                        _ -> error $ "unhandled type/filter for composite/non id primary keys pfilter=" ++ show pfilter ++ " persistList="++show allVals
                 (True, Just pdef, _) -> error $ "unhandled error for composite/non id primary keys pfilter=" ++ show pfilter ++ " persistList=" ++ show allVals ++ " pdef=" ++ show pdef

                 _ ->   case (isNull, pfilter, varCount) of
                            (True, Eq, _) -> (name <> " IS NULL", [])
                            (True, Ne, _) -> (name <> " IS NOT NULL", [])
                            (False, Ne, _) -> (T.concat
                                [ "("
                                , name
                                , " IS NULL OR "
                                , name
                                , " <> "
                                , qmarks
                                , ")"
                                ], notNullVals)
                            -- We use 1=2 (and below 1=1) to avoid using TRUE and FALSE, since
                            -- not all databases support those words directly.
                            (_, In, 0) -> ("1=2" <> orNullSuffix, [])
                            (False, In, _) -> (name <> " IN " <> qmarks <> orNullSuffix, allVals)
                            (True, In, _) -> (T.concat
                                [ "("
                                , name
                                , " IS NULL OR "
                                , name
                                , " IN "
                                , qmarks
                                , ")"
                                ], notNullVals)
                            (_, NotIn, 0) -> ("1=1", [])
                            (False, NotIn, _) -> (T.concat
                                [ "("
                                , name
                                , " IS NULL OR "
                                , name
                                , " NOT IN "
                                , qmarks
                                , ")"
                                ], notNullVals)
                            (True, NotIn, _) -> (T.concat
                                [ "("
                                , name
                                , " IS NOT NULL AND "
                                , name
                                , " NOT IN "
                                , qmarks
                                , ")"
                                ], notNullVals)
                            _ -> (name <> showSqlFilter pfilter <> "?" <> orNullSuffix, allVals) 

      where
        isCompFilter Lt = True
        isCompFilter Le = True
        isCompFilter Gt = True
        isCompFilter Ge = True
        isCompFilter _ =  False
        
        wrapSql sqlcl = "(" <> sqlcl <> ")"
        fromPersistList (PersistList xs) = xs
        fromPersistList other = error $ "expected PersistList but found " ++ show other
        
        filterValueToPersistValues :: forall a.  PersistField a => Either a [a] -> [PersistValue]
        filterValueToPersistValues v = map toPersistValue $ either return id v

        orNullSuffix =
            case orNull of
                OrNullYes -> mconcat [" OR ", name, " IS NULL"]
                OrNullNo -> ""

        isNull = any (== PersistNull) allVals
        notNullVals = filter (/= PersistNull) allVals
        allVals = filterValueToPersistValues value
        tn = connEscapeName conn $ entityDB
           $ entityDef $ dummyFromFilts [Filter field value pfilter]
        name =
            (if includeTable
                then ((tn <> ".") <>)
                else id)
            $ connEscapeName conn $ fieldDB $ persistFieldDef field
        qmarks = case value of
                    Left _ -> "?"
                    Right x ->
                        let x' = filter (/= PersistNull) $ map toPersistValue x
                         in "(" <> T.intercalate "," (map (const "?") x') <> ")"
        varCount = case value of
                    Left _ -> 1
                    Right x -> length x
        showSqlFilter Eq = "="
        showSqlFilter Ne = "<>"
        showSqlFilter Gt = ">"
        showSqlFilter Lt = "<"
        showSqlFilter Ge = ">="
        showSqlFilter Le = "<="
        showSqlFilter In = " IN "
        showSqlFilter NotIn = " NOT IN "
        showSqlFilter (BackendSpecificFilter s) = s
