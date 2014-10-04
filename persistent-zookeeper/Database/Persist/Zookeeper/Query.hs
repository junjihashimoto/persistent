{-# LANGUAGE FlexibleContexts, UndecidableInstances #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# OPTIONS_GHC -fno-warn-orphans #-}
module Database.Persist.Zookeeper.Query
       where

import Database.Persist
import Data.Monoid
import Data.List hiding (delete)
import Control.Applicative
import Control.Monad.IO.Class (MonadIO (..))
import Control.Monad.Trans.Control (MonadBaseControl)
import Control.Monad.Trans.Class
import qualified Data.Text as T
import qualified Database.Zookeeper as Z
import Database.Persist.Zookeeper.Config
import Database.Persist.Zookeeper.Internal
import Database.Persist.Zookeeper.Store
import qualified Data.Map as M
import Data.Conduit

filter2path :: (PersistEntity val) => [Filter val] -> String 
filter2path filterList = entity2path $ dummyFromFList filterList

getMap :: PersistEntity val => val -> M.Map T.Text PersistValue
getMap val =  M.fromList $ getList val
getList :: PersistEntity val => val -> [(T.Text,PersistValue)]
getList val =  
  let fields = fmap toPersistValue (toPersistFields val)
      in zip (getFieldsName val) fields
getFieldsName :: (PersistEntity val) => val -> [T.Text]
getFieldsName val =  fmap (unDBName.fieldDB) $ entityFields $ entityDef $ Just val
getFieldName :: (PersistEntity val,PersistField typ) => EntityField val typ -> T.Text
getFieldName field =  unDBName $ fieldDB $ persistFieldDef $ field
fieldval :: (PersistEntity val,PersistField typ) => EntityField val typ -> val -> PersistValue
fieldval field val =  (getMap val) M.! (getFieldName field)


updateEntity :: PersistEntity val =>  val -> [Update val] -> Either T.Text val
updateEntity val upds = 
  fromPersistValues $ map snd $ foldl updateVals (getList val) upds


updateVals :: PersistEntity val =>  [(T.Text,PersistValue)] -> Update val -> [(T.Text,PersistValue)]
updateVals [] _ = []
updateVals ((k,v):xs) u@(Update field _ _) = 
  if getFieldName field == k
    then (k,updateVal v u):xs
    else (k,v):updateVals xs u

updateVal :: PersistEntity val =>  PersistValue -> Update val -> PersistValue
updateVal _v (Update _ val upd) = 
  case upd of
    Assign -> toPersistValue val
    _ -> error "not support"
    -- Add -> (+) <$> v <$> toPersistValue val 
    -- Subtract -> v - toPersistValue val 
    -- Multiply -> v * toPersistValue val 
    -- Divide -> v `div` toPersistValue val 


instance (Applicative m, Functor m, MonadIO m, MonadBaseControl IO m) => PersistQuery (ZookeeperT m) where
  update key valList = do
    va <- get key
    case va of
      Nothing -> return ()
      Just v ->
        case updateEntity v valList of
          Right v' -> 
            replace key v'
          Left v' -> error $ show v'
  updateWhere filterList valList = do
    (selectKeys filterList []) $$ loop
    where
      loop = do
        key <- await
        case key of
          Just key' -> do
            lift $ update key' valList
            loop
          Nothing ->
            return ()
  deleteWhere filterList = do
    (str::[String]) <- execZookeeperT $ \zk -> do
      Z.getChildren zk (filter2path filterList) Nothing
    loop str
    where
      loop [] = return ()
      loop (x:xs) = do
        let key = txtToKey $ T.pack $ (filter2path filterList) <> "/" <> x
        va <- get key
        case va of
          Nothing -> return ()
          Just v -> do
            let (chk,_,_) = filterClause v filterList 
            if chk
              then delete key
              else return ()
        loop xs
  selectSource filterList [] = do
    (str::[String]) <- lift $ execZookeeperT $ \zk -> do
      Z.getChildren zk (filter2path filterList) Nothing
    loop str
    where
      loop [] = return ()
      loop (x:xs) = do
        let key = txtToKey $ T.pack $ (filter2path filterList) <> "/" <> x
        va <- get key
        case va of
          Nothing -> return ()
          Just v -> do
            let (chk,_,_) = filterClause v filterList 
            if chk
              then yield $ Entity key v
              else return ()
        loop xs
  selectFirst filterList selectOpts =  do
    (selectSource filterList selectOpts) $$ do
      val <- await
      case val of
        Just val' -> return $ Just val'
        Nothing -> return Nothing
  selectKeys filterList [] = do 
    (str::[String]) <- lift $ execZookeeperT $ \zk -> 
      Z.getChildren zk (filter2path filterList) Nothing
    loop str
    where
      loop [] = return ()
      loop (x:xs) = do
        let key = txtToKey $ T.pack $ (filter2path filterList) <> "/" <> x
        va <- get key
        case va of
          Nothing -> return ()
          Just v -> do
            let (chk,_,_) = filterClause v filterList 
            if chk
              then yield key
              else return ()
        loop xs
  count filterList = do
    v <- selectList filterList []
    return $ length v

  
dummyFromFilts :: [Filter v] -> Maybe v
dummyFromFilts _ = Nothing

data OrNull = OrNullYes | OrNullNo


filterClauseHelper :: PersistEntity val
             => Bool -- ^ include WHERE?
             -> OrNull
             -> val
             -> [Filter val]
             -> (Bool, T.Text, [PersistValue])
filterClauseHelper includeWhere orNull val filters =
    (bool, if not (T.null sql) && includeWhere
            then " WHERE " <> sql
            else sql, vals)
  where
    (bool, sql, vals) = combineAND filters
    combineAND = combine " AND " (&&)
    combineOR = combine " OR " (||)

    combine s op fs =
        (foldr1 op c ,T.intercalate s $ map wrapP a, mconcat b)
      where
        (c, a, b) = unzip3 $ map go fs
        wrapP x = T.concat ["(", x, ")"]
    go (BackendFilter _) = error "BackendFilter not expected"
    go (FilterAnd []) = (True,"1=1", [])
    go (FilterAnd fs) = combineAND fs
    go (FilterOr []) = (True,"1=0", [])
    go (FilterOr fs)  = combineOR fs
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
                           let sqlcl=T.intercalate " and " (map (\_a -> showSqlFilter pfilter <> T.pack (show xs) )  (primaryFields pdef))
                           in (True, wrapSql sqlcl,xs)
                        ([PersistList xs], Ne, _) -> 
                           let sqlcl=T.intercalate " or " (map (\_a -> showSqlFilter pfilter <> "?1 ")  (primaryFields pdef))
                           in (True, wrapSql sqlcl,xs)
                        (_, In, _) -> 
                           let xxs = transpose (map fromPersistList allVals)
                               sqls=map (\(_a,xs) -> showSqlFilter pfilter <> "(" <> T.intercalate "," (replicate (length xs) " ?2") <> ") ") (zip (primaryFields pdef) xxs)
                           in (True, wrapSql (T.intercalate " and " (map wrapSql sqls)), concat xxs)
                        (_, NotIn, _) -> 
                           let xxs = transpose (map fromPersistList allVals)
                               sqls=map (\(_a,xs) -> showSqlFilter pfilter <> "(" <> T.intercalate "," (replicate (length xs) " ?3") <> ") ") (zip (primaryFields pdef) xxs)
                           in (True, wrapSql (T.intercalate " or " (map wrapSql sqls)), concat xxs)
                        ([PersistList xs], _, True) -> 
                           let zs = tail (inits (primaryFields pdef))
                               sql1 = map (\b -> wrapSql (T.intercalate " and " (map (\(i,a) -> sql2 (i==length b) a) (zip [1..] b)))) zs
                               sql2 islast _a = (if islast then showSqlFilter pfilter else showSqlFilter Eq) <> "?4 "
                               sqlcl = T.intercalate " or " sql1
                           in (True, wrapSql sqlcl, concat (tail (inits xs)))
                        (_, BackendSpecificFilter _, _) -> error "unhandled type BackendSpecificFilter for composite/non id primary keys"
                        _ -> error $ "unhandled type/filter for composite/non id primary keys pfilter=" ++ show pfilter ++ " persistList="++show allVals
                 (True, Just pdef, _) -> error $ "unhandled error for composite/non id primary keys pfilter=" ++ show pfilter ++ " persistList=" ++ show allVals ++ " pdef=" ++ show pdef

                 _ ->   case (isNull, pfilter, varCount) of
                            -- (True, Eq, _) -> (True, name <> " IS NULL", [])
                            -- (True, Ne, _) -> (True, name <> " IS NOT NULL", [])
                            -- (False, Ne, _) -> (True, T.concat
                            --     [ "("
                            --     , name
                            --     , " IS NULL OR "
                            --     , name
                            --     , " <> "
                            --     , qmarks 0
                            --     , ")"
                            --     ], notNullVals)
                            -- -- We use 1=2 (and below 1=1) to avoid using TRUE and FALSE, since
                            -- -- not all databases support those words directly.
                            -- (_, In, 0) -> (True, "1=2" <> orNullSuffix, [])
                            -- (False, In, _) -> (True, name <> " IN " <> qmarks 1 <> orNullSuffix, allVals)
                            -- (True, In, _) -> (True, T.concat
                            --     [ "("
                            --     , name
                            --     , " IS NULL OR "
                            --     , name
                            --     , " IN "
                            --     , qmarks 2
                            --     , ")"
                            --     ], notNullVals)
                            -- (_, NotIn, 0) -> (True, "1=1", [])
                            -- (False, NotIn, _) -> (True, T.concat
                            --     [ "("
                            --     , name
                            --     , " IS NULL OR "
                            --     , name
                            --     , " NOT IN "
                            --     , qmarks 3
                            --     , ")"
                            --     ], notNullVals)
                            -- (True, NotIn, _) -> (True, T.concat
                            --     [ "("
                            --     , name
                            --     , " IS NOT NULL AND "
                            --     , name
                            --     , " NOT IN "
                            --     , qmarks 4
                            --     , ")"
                            --     ], notNullVals)
                            _ -> (showSqlFilter' pfilter (fieldval field val) allVals, 
                                  name <> ":"
                                  <> T.pack (show (fieldval field val)) <> ":" 
                                  <> showSqlFilter pfilter
                                  <> T.pack (show (showSqlFilter' pfilter (fieldval field val) allVals))
                                  <> "?5:" <> T.pack (show allVals) <> orNullSuffix, allVals) 
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
        name = unDBName $ fieldDB $ persistFieldDef field
        qmarks :: Int -> T.Text
        qmarks v = case value of
                     Left _ -> ("?6:" <> (T.pack $ show v))
                     Right x ->
                        let x' = filter (/= PersistNull) $ map toPersistValue x
                         in "(" <> T.intercalate "," (map (const "?7") x') <> ")"
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
        showSqlFilter' :: PersistFilter -> PersistValue -> [PersistValue] -> Bool
        showSqlFilter' Eq a b = (==) a (head b)
        showSqlFilter' Ne a b = (/=) a (head b)
        showSqlFilter' Gt a b = (>)  a (head b)
        showSqlFilter' Lt a b = (<)  a (head b)
        showSqlFilter' Ge a b = (>=) a (head b)
        showSqlFilter' Le a b = (<=) a (head b)
        showSqlFilter' In _ [] = False
        showSqlFilter' In a (x:xs) = if a==x then True else showSqlFilter' In a xs
        showSqlFilter' NotIn _ [] = True
        showSqlFilter' NotIn a (x:xs) = if a==x then False else showSqlFilter' NotIn a xs
        showSqlFilter' (BackendSpecificFilter _s) _ _ =  error "not supported"


filterClause :: PersistEntity val
             => val
             -> [Filter val]
             -> (Bool, T.Text, [PersistValue])
filterClause val = filterClauseHelper True OrNullNo val
